// Тесты триажа мутантов CI-прогона 09.11.2026 (половина A): добивки
// renders.go — точные значения парсера exposition (границы скобок/кавычек
// меток с экранами, «}» в кавычках, «{» в начале строки, пустые строки и
// пробельные, номера строк ошибок, обрезка « #»-хвоста, экспоненты),
// unescape значений (хвостовой слэш, экраны \\, \", \n, неизвестный),
// агрегация add (суммы по движкам, атрибуция RSS, пустой engine),
// rateOf/observe (окно 0 — темп 0, не Inf; рестарт; движения нет), доля
// успешности/тарпитов с нулевым знаменателем (nil, не NaN), fallback-рейт
// с tasks=1, статус экспозиции ≠ 200, кривой URL, лимит тела.
package dashboard

import (
	"context"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

// TestPromLabelsEnd — индекс закрывающей скобки с кавычками и экранами.
func TestPromLabelsEnd(t *testing.T) {
	cases := []struct {
		s    string
		want int
	}{
		{`{a="1"} 5`, 6},
		{`{a="x}y"} 5`, 8}, // } внутри кавычек — часть значения
		{`{a="\""} 5`, 7},  // экранированная кавычка
		{`{a="\}"`, -1},    // экран съедает закрывающую скобку
		{`{a="1`, -1},      // не закрыта
		{`{}`, 1},          // пустые метки
		{`{a="x\\"} 1`, 8}, // экранированный слэш
		{`{a\} 5`, 3},      // экран вне кавычек НЕ экранируется: скобка закрывает
		{`{"x"="1"} 5`, 8}, // кавычка сразу после скобки
	}
	for _, c := range cases {
		if got := promLabelsEnd(c.s, 0); got != c.want {
			t.Errorf("promLabelsEnd(%q) = %d, want %d", c.s, got, c.want)
		}
	}
}

// TestParsePromLabels — метки: пары, запятые/кавычки в значениях,
// экраны, пустая строка, ошибки пар.
func TestParsePromLabels(t *testing.T) {
	// Пустая строка/пробел — нет меток (nil).
	for _, s := range []string{"", "  "} {
		labels, err := parsePromLabels(s)
		if err != nil || labels != nil {
			t.Errorf("parsePromLabels(%q) = (%v, %v), want nil,nil", s, labels, err)
		}
	}
	labels, err := parsePromLabels(`a="1",b="two"`)
	if err != nil || !reflect.DeepEqual(labels, map[string]string{"a": "1", "b": "two"}) {
		t.Errorf("пары = (%v, %v)", labels, err)
	}
	// Запятая и кавычка в значении.
	labels, err = parsePromLabels(`a="x,y",b="q\"r"`)
	if err != nil || labels["a"] != "x,y" || labels["b"] != `q"r` {
		t.Errorf("значения с запятой/кавычкой = (%v, %v)", labels, err)
	}
	// Экраны: \\ → слэш, \n → n (текущий контракт), неизвестный — буквально.
	labels, err = parsePromLabels(`a="p\\ath",b="li\ne",c="e\x"`)
	if err != nil || labels["a"] != `p\ath` || labels["b"] != "line" || labels["c"] != `e\x` {
		t.Errorf("экраны = (%v, %v)", labels, err)
	}
	// Хвостовой слэш в значении — буквальный (незавершённый экран).
	labels, err = parsePromLabels(`a="trail\"`)
	if err != nil || labels["a"] != `trail\` {
		t.Errorf("хвостовой экран = (%v, %v)", labels, err)
	}
	// Пустое значение — валидно (граница len(v) < 2).
	labels, err = parsePromLabels(`a=""`)
	if err != nil || labels["a"] != "" {
		t.Errorf("пустое значение = (%v, %v)", labels, err)
	}
	// Ошибки формы: значение без кавычек, незакрытая кавычка, без «=»,
	// хвостовой незавершённый экран (ошибка формы, не паника), пара
	// только из кавычки, значение с закрывающей кавычкой без
	// открывающей.
	for _, bad := range []string{`a=1`, `a="1`, `a`, `a="1",b`, `a="x\`, `a="`, `a=xy"`} {
		if _, err := parsePromLabels(bad); err == nil {
			t.Errorf("%q: ошибки нет, want форма метки", bad)
		}
	}
	// Пара с невалидным значением до запятой: ошибка flush на
	// разделителе (не глотается последующими парами).
	if _, err := parsePromLabels(`a=1,b="2"`); err == nil {
		t.Error("битая пара до запятой: want ошибку на месте")
	}
	// Пустая пара между запятыми/в хвосте игнорируется (не ошибка).
	labels, err = parsePromLabels(`a="1",,b="2"`)
	if err != nil || len(labels) != 2 || labels["a"] != "1" || labels["b"] != "2" {
		t.Errorf("пустая пара: (%v, %v)", labels, err)
	}
	labels, err = parsePromLabels(`a="1",`)
	if err != nil || len(labels) != 1 {
		t.Errorf("хвостовая запятая: (%v, %v)", labels, err)
	}
	// Пробелы вокруг пары обрезаются (хвостовой пробел не ломает
	// закрывающую кавычку значения).
	labels, err = parsePromLabels(`a="1" `)
	if err != nil || labels["a"] != "1" {
		t.Errorf("пара с хвостовым пробелом: (%v, %v)", labels, err)
	}
	// Экран вне кавычек не экранирует запятую: пара обрывается и
	// признаётся битой; имя с запятой так не собрать.
	if _, err := parsePromLabels(`a\,b="1"`); err == nil {
		t.Error("экран вне кавычек: want ошибка формы пары")
	}
	// Экранированная кавычка в значении держит кавычку открытой:
	// запятая за ней — часть значения, вторая пара не выделяется.
	labels, err = parsePromLabels(`a="q\",b="2"`)
	if err != nil || len(labels) != 1 || labels["a"] != `q",b="2` {
		t.Errorf("экран внутри кавычек: (%v, %v)", labels, err)
	}
}

// TestPromUnescape — экраны значений и хвостовой слэш (не паникует).
func TestPromUnescape(t *testing.T) {
	cases := map[string]string{
		`p\\ath`: `p\ath`,
		`q\"r`:   `q"r`,
		`li\ne`:  "line",
		`e\x`:    `e\x`,
		`trail\`: `trail\`,
		`plain`:  "plain",
		`\`:      `\`,
		`\n`:     "n",
	}
	for in, want := range cases {
		if got := promUnescape(in); got != want {
			t.Errorf("promUnescape(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestParsePromExpositionExact — точный разбор канонической экспозиции:
// имена, метки (включая имя из «{» в начале строки), значения
// (экспонента, отрицательные), пропуски HELP/TYPE/пустых строк.
func TestParsePromExpositionExact(t *testing.T) {
	expo := "# HELP m Справка\n" +
		"# TYPE m counter\n" +
		"\n" +
		"   \n" +
		"m{a=\"1\",engine=\"moli\"} 1000\n" +
		"m 1e3\n" +
		"m{x=\"1\"} -2.5\n" +
		"m{} 3\n" +
		"{anon=\"yes\"} 7\n" +
		"m 5 # комментарий\n"
	samples, err := parsePromExposition(strings.NewReader(expo))
	if err != nil {
		t.Fatalf("разбор: %v", err)
	}
	want := []promSample{
		{Name: "m", Labels: map[string]string{"a": "1", "engine": "moli"}, Value: 1000},
		{Name: "m", Labels: nil, Value: 1000},
		{Name: "m", Labels: map[string]string{"x": "1"}, Value: -2.5},
		{Name: "m", Labels: nil, Value: 3},
		{Name: "", Labels: map[string]string{"anon": "yes"}, Value: 7},
		{Name: "m", Labels: nil, Value: 5},
	}
	if !reflect.DeepEqual(samples, want) {
		t.Errorf("сэмплы =\n%+v\nwant:\n%+v", samples, want)
	}
}

// TestParsePromExpositionErrorLines — ошибки формы несут точный номер
// строки и различают «нет значения» и «значение не число».
func TestParsePromExpositionErrorLines(t *testing.T) {
	cases := []struct {
		name    string
		expo    string
		wantSub string
	}{
		{"метки не закрыты (строка 2)", "# T x\nm{a=\"1 5\n", "строка 2: метки без закрывающей"},
		{"нет значения (строка 1)", "m{x=\"1\"}\n", "строка 1: нет значения"},
		{"нет значения после решётки", "m #x\n", "строка 1: нет значения"},
		{"значение не число (строка 3)", "m 1\nm 2\nm abc\n", "строка 3: значение"},
		{"метка без кавычек (строка 1)", "m{x=1} 5\n", "строка 1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parsePromExposition(strings.NewReader(tc.expo))
			if err == nil || !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("ошибка = %v, want содержит %q", err, tc.wantSub)
			}
		})
	}
}

// errReader — читатель, отдающий префикс и затем ошибку (обрыв
// соединения посреди экспозиции).
type errReader struct {
	prefix []byte
	err    error
	done   bool
}

func (r *errReader) Read(p []byte) (int, error) {
	if len(r.prefix) > 0 {
		n := copy(p, r.prefix)
		r.prefix = r.prefix[n:]
		return n, nil
	}
	if !r.done {
		r.done = true
		return 0, r.err
	}
	return 0, r.err
}

// TestParsePromExpositionReaderError — ошибка чтения посреди потока —
// «чтение экспозиции» с исходной (не тихий усечённый разбор).
func TestParsePromExpositionReaderError(t *testing.T) {
	r := &errReader{prefix: []byte("m 1\n"), err: errors.New("обрыв соединения")}
	if _, err := parsePromExposition(r); err == nil ||
		!strings.Contains(err.Error(), "чтение экспозиции") ||
		!strings.Contains(err.Error(), "обрыв") {
		t.Errorf("ошибка чтения = %v, want «чтение экспозиции» с исходной", err)
	}
}

// TestRenderBodyLimitPin — лимит чтения тела экспозиции.
func TestRenderBodyLimitPin(t *testing.T) {
	if renderBodyLimit != 4<<20 {
		t.Errorf("renderBodyLimit = %d, want 4 МиБ", renderBodyLimit)
	}
}

// TestRenderCountersAdd — сведение сэмплов: суммы по движкам и исходам
// (две записи одного движка в одной экспозиции), страницы этапа,
// атрибуция RSS движку первой rendered-true-серии с engine (пустой
// engine до валидной записи и чужие имена приборов не мешают); вторая
// экспозиция того же движка суммируется.
func TestRenderCountersAdd(t *testing.T) {
	var agg renderCounters
	agg.rendered, agg.finalized, agg.tarpits = map[string]float64{}, map[string]float64{}, map[string]float64{}
	agg.consumed, agg.rss = map[string]float64{}, map[string]float64{}
	agg.add([]promSample{
		// Чужой прибор с rendered=true и engine — не источник движка.
		{Name: fbPublishedMetric, Labels: map[string]string{"rendered": "true", "engine": "spooky"}},
		// records с rendered=false — движок не открывает.
		{Name: renderRecordsMetric, Labels: map[string]string{"rendered": "false", "engine": "zzz"}},
		{Name: renderRecordsMetric, Labels: map[string]string{"rendered": "true"}}, // пустой engine
		{Name: renderRecordsMetric, Labels: map[string]string{"rendered": "true", "engine": "moli"}, Value: 100},
		{Name: renderRecordsMetric, Labels: map[string]string{"rendered": "true", "engine": "moli"}, Value: 50}, // сумма 150
		// Ещё одна rendered-true серия другого движка ПОСЛЕ рабочей —
		// атрибуция остаётся за первой (break).
		{Name: renderRecordsMetric, Labels: map[string]string{"rendered": "true", "engine": "late"}},
		{Name: renderRecordsMetric, Labels: map[string]string{"rendered": "false", "engine": "moli"}, Value: 9},
		{Name: renderFinalizedMetric, Labels: map[string]string{"engine": "moli"}, Value: 10},
		{Name: renderFinalizedMetric, Labels: map[string]string{"engine": "moli"}, Value: 5}, // 15
		{Name: renderTarpitMetric, Labels: map[string]string{"engine": "moli"}, Value: 2},
		{Name: renderTarpitMetric, Labels: map[string]string{"engine": "moli"}, Value: 3}, // 5
		{Name: fbPublishedMetric, Labels: map[string]string{"reason": "timeout"}, Value: 3},
		{Name: fbPublishedMetric, Labels: map[string]string{"reason": "other"}, Value: 4}, // 7
		{Name: fbConsumedMetric, Labels: map[string]string{"result": "rendered"}, Value: 1},
		{Name: fbConsumedMetric, Labels: map[string]string{"result": "rendered"}, Value: 2}, // 3
		{Name: stageTasksMetric, Labels: map[string]string{"stage": "render"}, Value: 40},
		{Name: stageTasksMetric, Labels: map[string]string{"stage": "http"}, Value: 999}, // чужой этап
		{Name: processRSSMetric, Labels: nil, Value: 123},                                // → движку moli
	})
	// Вторая экспозиция того же движка (второй воркер): RSS и прочие
	// кумулятивы суммируются по экспозициям.
	agg.add([]promSample{
		{Name: renderRecordsMetric, Labels: map[string]string{"rendered": "true", "engine": "moli"}, Value: 25},
		{Name: renderTarpitMetric, Labels: map[string]string{"engine": "moli"}, Value: 7},
		{Name: processRSSMetric, Labels: nil, Value: 100},
	})
	if agg.rendered["moli"] != 175 {
		t.Errorf("rendered[moli] = %v, want 175 (100+50+25)", agg.rendered["moli"])
	}
	if agg.finalized["moli"] != 15 {
		t.Errorf("finalized[moli] = %v, want 15", agg.finalized["moli"])
	}
	if agg.tarpits["moli"] != 12 {
		t.Errorf("tarpits[moli] = %v, want 12 (2+3+7)", agg.tarpits["moli"])
	}
	if agg.published != 7 {
		t.Errorf("published = %v, want 7 (3+4)", agg.published)
	}
	if agg.consumed["rendered"] != 3 {
		t.Errorf("consumed[rendered] = %v, want 3", agg.consumed["rendered"])
	}
	if agg.tasks != 40 {
		t.Errorf("tasks = %v, want 40 (только stage=render)", agg.tasks)
	}
	if agg.rss["moli"] != 223 {
		t.Errorf("rss[moli] = %v, want 223 (123+100)", agg.rss["moli"])
	}
	for _, ghost := range []string{"spooky", "zzz", "late"} {
		if _, ok := agg.rss[ghost]; ok {
			t.Errorf("rss приписана чужому движку %s", ghost)
		}
	}
}

// TestRateOfBoundaries — окно 0/отрицательное и не-рост — темп 0;
// положительное окно — точная доля.
func TestRateOfBoundaries(t *testing.T) {
	if got := rateOf(10, 20, 0); got != 0 {
		t.Errorf("rateOf dt=0 = %v, want 0 (не Inf)", got)
	}
	if got := rateOf(10, 20, -time.Second); got != 0 {
		t.Errorf("rateOf dt<0 = %v, want 0", got)
	}
	if got := rateOf(20, 20, time.Second); got != 0 {
		t.Errorf("rateOf без роста = %v, want 0", got)
	}
	if got := rateOf(10, 20, 10*time.Second); got != 1 {
		t.Errorf("rateOf = %v, want 1/с", got)
	}
	// Окно в одну наносекунду — законное положительное: рост считается
	// (мутант «dt <= 1» глотал бы его в ноль; 10 за 1 нс = 1e10/с).
	if got := rateOf(10, 20, time.Nanosecond); got < 1e9 {
		t.Errorf("rateOf dt=1нс = %v, want ~1e10/с (положительное окно)", got)
	}
}

// TestRenderRatesObserve — темп с окном 0 (часы не двигались) — 0, не
// Inf; окно отрицательное (часы ушли назад) — тоже 0; дробное окно
// (полсекунды) — темп считается.
func TestRenderRatesObserve(t *testing.T) {
	st := renderRateState{seen: true, at: time.Unix(1000, 0),
		rendered: map[string]float64{"moli": 10}, published: 5, consumed: 3}
	cur := renderCounters{
		rendered:  map[string]float64{"moli": 20},
		published: 7,
		consumed:  map[string]float64{"rendered": 5},
	}
	// Часы не двигались: dt = 0 — темпы 0.
	out := st.observe(cur, time.Unix(1000, 0))
	if out.rendered["moli"] != 0 || out.published != 0 || out.consumed != 0 {
		t.Errorf("dt=0: темпы = %+v, want нули", out)
	}
	// Часы ушли назад: dt < 0 — тоже нули (не отрицательные).
	st2 := renderRateState{seen: true, at: time.Unix(2000, 0),
		rendered: map[string]float64{"moli": 10}, published: 5, consumed: 3}
	out = st2.observe(cur, time.Unix(1000, 0))
	if out.rendered["moli"] != 0 || out.published != 0 || out.consumed != 0 {
		t.Errorf("dt<0: темпы = %+v, want нули", out)
	}
	// Дробное окно 0.5 с: темп = дельта/0.5 (удвоенная дельта) — окно
	// короче секунды полноценно работает; потребление — сумма по
	// исходам (две ветви consumed).
	st3 := renderRateState{seen: true, at: time.Unix(1000, 0),
		rendered: map[string]float64{"moli": 10}, published: 5, consumed: 3}
	cur2 := renderCounters{
		rendered:  map[string]float64{"moli": 20},
		published: 7,
		consumed:  map[string]float64{"rendered": 5, "finalized": 1},
	}
	out = st3.observe(cur2, time.Unix(1000, 0).Add(500*time.Millisecond))
	if out.rendered["moli"] != 20 || out.published != 4 || out.consumed != 6 {
		t.Errorf("dt=0.5с: темпы = %+v, want 20/4/6 (consumed = (5+1−3)/0.5)", out)
	}
}

// TestCollectRenderZeroDenominators — движок только с RSS (завершений
// нет): успешность и тарпит-рейт — nil «—», не NaN/0.
func TestCollectRenderZeroDenominators(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(
			"crawler_render_records_total{rendered=\"true\",engine=\"only-rss\"} 0\n" +
				"process_resident_memory_bytes " + formatFloat(1<<20) + "\n"))
	}))
	defer srv.Close()
	coll, _ := newRenderCollector(t, &renderLens{}, []string{srv.URL})
	snap := coll.CollectOnce(context.Background())
	if snap.Render == nil || len(snap.Render.Engines) != 1 {
		t.Fatalf("engines = %+v", snap.Render)
	}
	e := snap.Render.Engines[0]
	if e.SuccessPct != nil {
		t.Errorf("успешность при нулевом знаменателе = %v, want nil", *e.SuccessPct)
	}
	if e.TarpitPct != nil {
		t.Errorf("тарпит-рейт при нулевом знаменателе = %v, want nil", *e.TarpitPct)
	}
}

// TestCollectRenderUnitDenominators — знаменатель 1 (минимальный
// ненулевой): доли считаются (0 % и 100 %), не гаснут nil'ом.
func TestCollectRenderUnitDenominators(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(
			"crawler_render_records_total{rendered=\"true\",engine=\"e1\"} 0\n" +
				"crawler_render_task_finalized_total{reason=\"timeout\",engine=\"e1\"} 1\n" +
				"crawler_render_tarpit_finalized_total{engine=\"e1\"} 0\n" +
				"crawler_stage_tasks_total{stage=\"render\"} 1\n"))
	}))
	defer srv.Close()
	coll, _ := newRenderCollector(t, &renderLens{}, []string{srv.URL})
	snap := coll.CollectOnce(context.Background())
	e := snap.Render.Engines[0]
	if e.SuccessPct == nil || *e.SuccessPct != 0 {
		t.Errorf("успешность при знаменателе 1 = %v, want 0%%", e.SuccessPct)
	}
	if e.TarpitPct == nil || *e.TarpitPct != 0 {
		t.Errorf("тарпит-рейт при знаменателе 1 = %v, want 0%%", e.TarpitPct)
	}
	// Страниц этапа ровно одна, публикаций нет: fallback-рейт — честный
	// 0 %, не «—».
	if snap.Render.Fallback.RatePct == nil || *snap.Render.Fallback.RatePct != 0 {
		t.Errorf("rate_pct при tasks=1 = %v, want 0%%", snap.Render.Fallback.RatePct)
	}
}

// TestFetchRenderMetricsBadURL — кривой URL — ошибка экспозиции (не
// паника на nil-запросе).
func TestFetchRenderMetricsBadURL(t *testing.T) {
	coll, _ := newRenderCollector(t, &renderLens{}, []string{"::://кривой"})
	if _, err := coll.fetchRenderMetrics(context.Background(), "::://кривой"); err == nil {
		t.Error("кривой URL: хочу ошибку экспозиции")
	}
}

// TestFetchRenderMetricsBadStatus — статус ≠ 200 с валидным телом —
// ошибка «статус», тело не разбирается как экспозиция.
func TestFetchRenderMetricsBadStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("metric_ok 1\n"))
	}))
	defer srv.Close()
	coll, _ := newRenderCollector(t, &renderLens{}, []string{srv.URL})
	_, err := coll.fetchRenderMetrics(context.Background(), srv.URL)
	if err == nil || !strings.Contains(err.Error(), "500") {
		t.Errorf("статус 500: %v, want ошибка со статусом", err)
	}
	// Секция целиком деградирует, render = nil.
	snap := coll.CollectOnce(context.Background())
	if snap.Render != nil || snap.Errors["render"] == "" {
		t.Errorf("render=%v errors=%v, want деградация секции", snap.Render, snap.Errors)
	}
}

// TestFetchRenderMetricsTruncatedBody — обрыв тела посреди ответа
// (Content-Length больше факта) — ошибка «чтение», не усечённый разбор.
func TestFetchRenderMetricsTruncatedBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "1000")
		_, _ = w.Write([]byte("m 1\n")) // тело короче обещанного — обрыв
	}))
	defer srv.Close()
	coll, _ := newRenderCollector(t, &renderLens{}, []string{srv.URL})
	_, err := coll.fetchRenderMetrics(context.Background(), srv.URL)
	if err == nil || !strings.Contains(err.Error(), "чтение") {
		t.Errorf("обрыв тела = %v, want ошибка чтения", err)
	}
}

// TestCollectRenderCountersNoURLs — адресов нет: пустой агрегат без
// обращения к источникам (nil-карты, без экспозиций).
func TestCollectRenderCountersNoURLs(t *testing.T) {
	coll, _ := newRenderCollector(t, &renderLens{}, nil)
	agg, err := coll.collectRenderCounters(context.Background())
	if err != nil {
		t.Fatalf("без адресов: %v", err)
	}
	if agg.rendered != nil || agg.finalized != nil || agg.tarpits != nil ||
		agg.consumed != nil || agg.rss != nil || agg.published != 0 || agg.tasks != 0 {
		t.Errorf("агрегат без адресов = %+v, want нулевой (nil-карты)", agg)
	}
}

// TestCollectRenderSameClockNoInf — два прохода с одними часами: темпы
// 0 (окно 0), не Inf/NaN — панели не рвутся всплеском.
func TestCollectRenderSameClockNoInf(t *testing.T) {
	expo := "crawler_render_records_total{rendered=\"true\",engine=\"moli\"} 100\n" +
		"crawler_render_fallback_published_total{reason=\"timeout\"} 5\n"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(expo))
	}))
	defer srv.Close()
	coll, _ := newRenderCollector(t, &renderLens{}, []string{srv.URL})
	for i := 0; i < 2; i++ {
		snap := coll.CollectOnce(context.Background())
		if snap.Render == nil {
			t.Fatalf("проход %d: render nil", i)
		}
		if math.IsInf(snap.Render.Engines[0].RenderedPerSec, 0) ||
			math.IsNaN(snap.Render.Engines[0].RenderedPerSec) {
			t.Fatalf("проход %d: темп = %v, want 0", i, snap.Render.Engines[0].RenderedPerSec)
		}
		if math.IsInf(snap.Render.Fallback.PublishedPerSec, 0) {
			t.Fatalf("проход %d: published темп = %v", i, snap.Render.Fallback.PublishedPerSec)
		}
	}
	// Повторный проход с живой секцией.
	if snap := coll.CollectOnce(context.Background()); snap.Render == nil {
		t.Fatal("секция умерла")
	}
}

// Юнит-тесты алерта мусорных content_hash очистки ingest (T-099) и его
// HTTP-источника: порог всплеска за окно, базирование первого наблюдения
// (старт мониторинга позже накопленного счётчика — не всплеск), рестарт
// ingest (счётчик начал заново — без ложного срабатывания), старение окна
// без продолжения потока, деградация источника, разбор экспозиции
// Prometheus и поведение зонда по HTTP (статусы, отсутствие метрики,
// отмена ctx). Живой зонд против /metrics ingest — интеграционный уровень.
package monitor

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// fakeBadHashes — источник счётчика по заготовке: значение меняется между
// проходами, очередь ошибок (пустая — ответ без ошибки).
type fakeBadHashes struct {
	mu    sync.Mutex
	val   int64
	errs  []error
	calls int
}

func (f *fakeBadHashes) BadHashes(context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if err := popErr(&f.errs); err != nil {
		return 0, err
	}
	return f.val, nil
}

func (f *fakeBadHashes) set(v int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.val = v
}

// --- алерт -------------------------------------------------------------------

// TestAlertsBadHashesThreshold — накопление дельт до порога окном:
// скачки ниже порога поодиночке не срабатывают, сумма за окно ≥
// AlertBadHashesThreshold — firing; без продолжения потока окно стареет
// и алерт снимается.
func TestAlertsBadHashesThreshold(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	src := &fakeBadHashes{}
	env.deps.BadHashes = src
	ctx := context.Background()

	// Первое наблюдение — база: накопленное до старта мониторинга
	// (100) не считается всплеском.
	src.set(100)
	if s := snapshotOf(t, env.Pass(ctx), AlertBadHashes); s.Status != AlertStatusOK {
		t.Fatalf("база: статус = %s (%s), want ok", s.Status, s.Detail)
	}

	// Рост по 5 и 3 — сумма 8 < 10: ок.
	src.set(105)
	if s := snapshotOf(t, env.Pass(ctx), AlertBadHashes); s.Status != AlertStatusOK {
		t.Errorf("сумма 8: статус = %s (%s), want ok", s.Status, s.Detail)
	}
	src.set(108)
	if s := snapshotOf(t, env.Pass(ctx), AlertBadHashes); s.Status != AlertStatusOK {
		t.Errorf("сумма 8: статус = %s (%s), want ok", s.Status, s.Detail)
	}

	// Ещё 4 — сумма 12 ≥ 10: всплеск.
	src.set(112)
	s := snapshotOf(t, env.Pass(ctx), AlertBadHashes)
	if s.Status != AlertStatusFiring {
		t.Fatalf("сумма 12: статус = %s (%s), want firing", s.Status, s.Detail)
	}
	for _, want := range []string{"112", "12", "10"} { // счётчик, сумма, порог
		if !strings.Contains(s.Detail, want) {
			t.Errorf("detail %q не содержит %s", s.Detail, want)
		}
	}

	// Держится следующий проход (сумма ещё в окне)…
	src.set(112)
	if s := snapshotOf(t, env.Pass(ctx), AlertBadHashes); s.Status != AlertStatusFiring {
		t.Errorf("окно ещё держит сумму: статус = %s (%s), want firing", s.Status, s.Detail)
	}

	// …и стареет без новых мусорных строк: после окна нулевых дельт
	// сумма обнуляется — снятие.
	for range AlertBadHashesWindow + 2 {
		if s := snapshotOf(t, env.Pass(ctx), AlertBadHashes); s.Status == AlertStatusOK {
			return
		}
	}
	t.Fatal("алерт не снят после старения окна без новых мусорных строк")
}

// TestBadHashesWindow — размер кольца дельт: окно ровно 15 значений —
// всплеск удерживается суммой до вытеснения последней (15-й) нулевой
// дельтой. Размер pinned литералом в начале: граница из константы
// мутировалась бы вместе с тестом и «следовала» за мутантом. Шаг записи
// отдельно не фиксируется: любой взаимно простой с длиной шаг даёт то же
// множество последних дельт (см. аннотацию в push).
func TestBadHashesWindow(t *testing.T) {
	if AlertBadHashesWindow != 15 {
		t.Fatalf("AlertBadHashesWindow = %d, want 15 (окно 15 проходов алертов)", AlertBadHashesWindow)
	}
	w := &badHashesWindow{}
	if sum := w.push(0); sum != 0 {
		t.Fatalf("первое наблюдение (база): сумма = %d, want 0", sum)
	}
	if sum := w.push(10); sum != 10 {
		t.Fatalf("всплеск +10: сумма = %d, want 10", sum)
	}
	// Окно 15: всплеск держится, пока 14 последующих дельт не вытеснили
	// его из кольца (без роста счётчика дельты нулевые).
	for i := 1; i <= AlertBadHashesWindow-1; i++ {
		if sum := w.push(10); sum != 10 {
			t.Fatalf("после %d нулевых дельт: сумма = %d, want 10 (окно ещё держит)", i, sum)
		}
	}
	// 15-я нулевая дельта перезаписывает ячейку всплеска — сумма 0.
	if sum := w.push(10); sum != 0 {
		t.Fatalf("окно освободилось: сумма = %d, want 0", sum)
	}
}

// TestAlertsBadHashesExactThreshold — сумма ровно AlertBadHashesThreshold —
// уже всплеск (≥, не «строго больше»); деталь называет окно — 15 проходов
// интервала тестовой среды (DefaultAlertsInterval 5м) = 1ч15м.
func TestAlertsBadHashesExactThreshold(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	src := &fakeBadHashes{}
	env.deps.BadHashes = src

	src.set(0)
	snapshotOf(t, env.Pass(context.Background()), AlertBadHashes) // база
	src.set(10)                                                   // одна дельта ровно 10
	s := snapshotOf(t, env.Pass(context.Background()), AlertBadHashes)
	if s.Status != AlertStatusFiring {
		t.Fatalf("сумма ровно 10: статус = %s (%s), want firing", s.Status, s.Detail)
	}
	for _, want := range []string{"1h15m0s", "10"} { // окно 15 × 5м, порог
		if !strings.Contains(s.Detail, want) {
			t.Errorf("detail %q не содержит %s", s.Detail, want)
		}
	}
}

// TestAlertsBadHashesRestart — рестарт ingest обнуляет счётчик: убывание
// даёт нулевую дельту, ложного всплеска нет (счётчик считается заново).
func TestAlertsBadHashesRestart(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	src := &fakeBadHashes{}
	env.deps.BadHashes = src
	ctx := context.Background()

	src.set(50) // база
	snapshotOf(t, env.Pass(ctx), AlertBadHashes)
	src.set(50) // тишина
	if s := snapshotOf(t, env.Pass(ctx), AlertBadHashes); s.Status != AlertStatusOK {
		t.Fatalf("тишина: статус = %s (%s), want ok", s.Status, s.Detail)
	}

	src.set(3) // рестарт ingest: счётчик меньше прежнего
	s := snapshotOf(t, env.Pass(ctx), AlertBadHashes)
	if s.Status != AlertStatusOK {
		t.Errorf("рестарт источника: статус = %s (%s), want ok без ложного всплеска", s.Status, s.Detail)
	}

	// Новый рост после рестарта считается от новой базы.
	src.set(20) // +17 ≥ порога
	if s := snapshotOf(t, env.Pass(ctx), AlertBadHashes); s.Status != AlertStatusFiring {
		t.Errorf("рост после рестарта: статус = %s (%s), want firing", s.Status, s.Detail)
	}
}

// TestAlertsBadHashesDegradation — без источника алерт «не измеряется»
// тихо; ошибка источника (ingest недоступен) — Warn и «не измеряется»,
// следующий проход жив.
func TestAlertsBadHashesDegradation(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	if s := snapshotOf(t, env.Pass(context.Background()), AlertBadHashes); s.Status != AlertStatusNotMeasured {
		t.Fatalf("без источника: статус = %s, want not_measured", s.Status)
	}

	src := &fakeBadHashes{errs: []error{errors.New("connection refused")}}
	env.deps.BadHashes = src
	s := snapshotOf(t, env.Pass(context.Background()), AlertBadHashes)
	if s.Status != AlertStatusNotMeasured {
		t.Fatalf("ошибка источника: статус = %s, want not_measured", s.Status)
	}
	if !strings.Contains(env.buf.String(), "level=WARN") || !strings.Contains(env.buf.String(), "connection refused") {
		t.Errorf("ошибка источника без Warn:\n%s", env.buf.String())
	}

	s = snapshotOf(t, env.Pass(context.Background()), AlertBadHashes)
	if s.Status != AlertStatusOK {
		t.Errorf("после ошибки статус = %s, want ok (цикл живёт)", s.Status)
	}
}

// --- HTTP-источник --------------------------------------------------------------

// TestIngestBadHashesHTTP — зонд читает счётчик из экспозиции /metrics:
// реальный формат (HELP/TYPE, соседние метрики с метками) и ошибки
// статуса/сети.
func TestIngestBadHashesHTTP(t *testing.T) {
	const exposition = "# HELP crawler_service_up Сервис запущен.\n" +
		"# TYPE crawler_service_up gauge\n" +
		"crawler_service_up{service=\"ingest\"} 1\n" +
		"# HELP crawler_ingest_bad_hashes_total Удалённые очисткой результаты.\n" +
		"# TYPE crawler_ingest_bad_hashes_total counter\n" +
		"crawler_ingest_bad_hashes_total{service=\"ingest\"} 42\n"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(exposition))
	}))
	defer srv.Close()

	got, err := (IngestBadHashes{URL: srv.URL}).BadHashes(context.Background())
	if err != nil {
		t.Fatalf("BadHashes: %v", err)
	}
	if got != 42 {
		t.Errorf("счётчик = %d, want 42", got)
	}

	// Некорректный URL (управляющий символ) — ошибка сборки запроса до
	// сети; значение ошибки — строго 0 (не ±1).
	got, err = (IngestBadHashes{URL: "http://ingest:9101/\n"}).BadHashes(context.Background())
	if err == nil || got != 0 {
		t.Errorf("ошибка URL: (%d, %v), want (0, ошибка)", got, err)
	}

	// Отсутствие метрики (старый ingest) — ошибка источника с именем.
	noMetric := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("crawler_service_up{service=\"ingest\"} 1\n"))
	}))
	defer noMetric.Close()
	if n, err := (IngestBadHashes{URL: noMetric.URL}).BadHashes(context.Background()); err == nil ||
		!strings.Contains(err.Error(), badHashesMetricName) || n != 0 {
		t.Errorf("нет метрики: (%d, %v), want (0, ошибка с именем метрики)", n, err)
	}

	// Не-200 — ошибка со статусом.
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer bad.Close()
	if n, err := (IngestBadHashes{URL: bad.URL}).BadHashes(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "503") || n != 0 {
		t.Errorf("статус 503: (%d, %v), want (0, ошибка со статусом)", n, err)
	}

	// Недоступный сервер — ошибка транспорта.
	dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := dead.URL
	dead.Close()
	if n, err := (IngestBadHashes{URL: url}).BadHashes(context.Background()); err == nil || n != 0 {
		t.Errorf("закрытый сервер: (%d, %v), want (0, ошибка)", n, err)
	}

	// Отмена ctx — ошибка контекста, не зависание.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (IngestBadHashes{URL: srv.URL}).BadHashes(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("отмена ctx: ошибка = %v, want context.Canceled", err)
	}
}

// roundTripFunc — транспорт с фиксированным ответом, без сети.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// closeTrackingBody — тело ответа с отметкой Close.
type closeTrackingBody struct {
	io.Reader
	closed bool
}

func (b *closeTrackingBody) Close() error {
	b.closed = true
	return nil
}

// TestIngestBadHashesClosesBody — тело ответа закрывается после разбора:
// единственный ресурс метода, утечка держала бы соединения ingest.
func TestIngestBadHashesClosesBody(t *testing.T) {
	body := &closeTrackingBody{Reader: strings.NewReader(
		"crawler_ingest_bad_hashes_total{service=\"ingest\"} 42\n")}
	cl := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       body,
			Header:     make(http.Header),
		}, nil
	})}

	got, err := (IngestBadHashes{URL: "http://ingest:9101/metrics", Client: cl}).BadHashes(context.Background())
	if err != nil {
		t.Fatalf("BadHashes: %v", err)
	}
	if got != 42 {
		t.Errorf("счётчик = %d, want 42", got)
	}
	if !body.closed {
		t.Error("тело ответа не закрыто")
	}
}

// TestParseBadHashesExposition — табличный разбор формата: метки, научная
// запись, штамп времени после значения, комментарии и мусор — мимо.
func TestParseBadHashesExposition(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		want    int64
		wantErr string // «» — успех
		wantHas string // значение-нарушитель в тексте ошибки («» — не важно)
	}{
		{
			"ряд с метками",
			"crawler_ingest_bad_hashes_total{service=\"ingest\"} 42\n",
			42, "", "",
		},
		{
			"научная запись",
			"crawler_ingest_bad_hashes_total 1.7e+06\n",
			1700000, "", "",
		},
		{
			"штамп времени после значения игнорируется",
			"crawler_ingest_bad_hashes_total{service=\"ingest\"} 7 1697760000000\n",
			7, "", "",
		},
		{
			"соседние метрики и HELP/TYPE мимо",
			"# HELP crawler_stage_tasks_total Задачи.\n" +
				"# TYPE crawler_stage_tasks_total counter\n" +
				"crawler_stage_tasks_total{service=\"dns-worker\",stage=\"dns\"} 3\n" +
				"# TYPE crawler_ingest_bad_hashes_total counter\n" +
				"crawler_ingest_bad_hashes_total{service=\"ingest\"} 5\n",
			5, "", "",
		},
		// Ноль — валидный счётчик (рестарт ingest): граница «v < 0»
		// включающая, отрицательные и не-числа — ошибка с названием
		// значения-нарушителя.
		{"нулевое значение", "crawler_ingest_bad_hashes_total 0\n", 0, "", ""},
		{"минус один", "crawler_ingest_bad_hashes_total -1\n", 0, "не счётчик", "-1"},
		{"имя без значения", "crawler_ingest_bad_hashes_total\n", 0, "отсутствует", ""},
		{"комментарий во втором поле", "crawler_ingest_bad_hashes_total #42\n", 0, "не счётчик", "#42"},
		{"метрики нет", "crawler_service_up 1\n", 0, "отсутствует", ""},
		{"значение не число", "crawler_ingest_bad_hashes_total abc\n", 0, "не счётчик", "abc"},
		{"отрицательное значение", "crawler_ingest_bad_hashes_total -3\n", 0, "не счётчик", "-3"},
		{"префикс чужого имени", "crawler_ingest_bad_hashes_total_extra 9\n", 0, "отсутствует", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseBadHashesExposition(strings.NewReader(tt.body))
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("ошибка = %v, want содержит %q", err, tt.wantErr)
				}
				if got != 0 {
					t.Fatalf("значение при ошибке = %d, want 0", got)
				}
				if tt.wantHas != "" && !strings.Contains(err.Error(), tt.wantHas) {
					t.Errorf("текст ошибки %q не называет значение-нарушитель %q", err.Error(), tt.wantHas)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseBadHashesExposition: %v", err)
			}
			if got != tt.want {
				t.Errorf("значение = %d, want %d", got, tt.want)
			}
		})
	}
}

// failReader — читатель с неисправимой ошибкой (обрыв транспорта во
// время чтения тела /metrics).
type failReader struct{ err error }

func (r *failReader) Read([]byte) (int, error) { return 0, r.err }

// TestParseBadHashesExpositionReadError — ошибка чтения тела всплывает из
// Scanner: «чтение экспозиции» с исходной причиной и нулевое значение.
func TestParseBadHashesExpositionReadError(t *testing.T) {
	got, err := parseBadHashesExposition(&failReader{err: errors.New("сброс соединения")})
	if err == nil ||
		!strings.Contains(err.Error(), "чтение экспозиции") ||
		!strings.Contains(err.Error(), "сброс соединения") {
		t.Fatalf("ошибка = %v, want «чтение экспозиции» с причиной", err)
	}
	if got != 0 {
		t.Errorf("значение = %d, want 0 при ошибке", got)
	}
}

// TestParseBadHashesExpositionBodyLimit — потолок чтения тела: метрика
// последней строкой ровно на границе badHashesBodyLimit читается (лимит
// включающий), за пределами — отрезается («метрика отсутствует»).
// Сам лимит pinned литералом 4<<20: константа в проверке мутировала бы
// вместе с мутантом и «следовала» за ним (T-M01, alerts).
func TestParseBadHashesExpositionBodyLimit(t *testing.T) {
	if badHashesBodyLimit != 4<<20 {
		t.Fatalf("badHashesBodyLimit = %d, want 4<<20 (4 МиБ)", badHashesBodyLimit)
	}
	const filler = "crawler_service_up{service=\"ingest\"} 1\n"
	const metric = "crawler_ingest_bad_hashes_total{service=\"ingest\"} 42\n"

	// Ровно на лимите: filler-строки, добивка точной длины и метрика
	// последней строкой — общий размер badHashesBodyLimit.
	var b strings.Builder
	for b.Len()+len(filler)+len(metric) <= badHashesBodyLimit {
		b.WriteString(filler)
	}
	rest := badHashesBodyLimit - b.Len() - len(metric)
	switch {
	case rest == 1:
		b.WriteString("\n") // пустая строка — тоже мимо парсера
	case rest >= 2:
		b.WriteString("#" + strings.Repeat("x", rest-2) + "\n")
	}
	b.WriteString(metric)
	if b.Len() != badHashesBodyLimit {
		t.Fatalf("тело на границе = %d байт, want %d", b.Len(), badHashesBodyLimit)
	}
	got, err := parseBadHashesExposition(strings.NewReader(b.String()))
	if err != nil || got != 42 {
		t.Fatalf("на границе лимита: (%d, %v), want (42, nil)", got, err)
	}

	// За лимитом: метрика в хвосте за badHashesBodyLimit — не читается.
	var over strings.Builder
	for over.Len()+len(filler) <= badHashesBodyLimit {
		over.WriteString(filler)
	}
	over.WriteString(filler) // хвост за лимитом
	over.WriteString(metric)
	if _, err := parseBadHashesExposition(strings.NewReader(over.String())); err == nil ||
		!strings.Contains(err.Error(), "отсутствует") {
		t.Fatalf("за лимитом: %v, want «метрика отсутствует»", err)
	}
}

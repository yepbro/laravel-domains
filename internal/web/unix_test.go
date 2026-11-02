// Тесты формы на unix-сокете (T-221): пир unix-сокета — доверенный
// прокси (вариант деплоя deploy/nginx/README.md §«Вариант unix-сокета»;
// RemoteAddr пира — «@» для безымянного клиента автобинда или путь
// сокета, никогда не IP). Критерии: X-Real-IP/X-Forwarded-For дают
// независимые бакеты рейт-лимита (до T-221 — один бакет на всех
// посетителей), пир без валидного заголовка — общий бакет-метка без
// записи не-IP в inet (INSERT не пытается, Warn в журнале), ошибка
// INSERT логируется, метрики формы регистрируются и отдаются реестром.
package web

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/checker"
	"crawler/internal/config"
	"crawler/internal/metrics"
)

// newUnixTestServer — httptest-сервер на unix-сокете во временном
// каталоге; возвращает клиент с диалером на сокет.
func newUnixTestServer(t *testing.T, h http.Handler) *http.Client {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "web.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	ts := httptest.NewUnstartedServer(h)
	ts.Listener = ln
	ts.Start()
	t.Cleanup(ts.Close)
	return &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", sock)
		},
	}}
}

// postFormUnix — POST формы через клиента с заголовками прокси
// (X-Real-IP/X-Forwarded-For); пустое значение — заголовок не ставится.
func postFormUnix(t *testing.T, c *http.Client, realIP, xff string) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, "http://unix/api/check-domain",
		strings.NewReader(`{"domain":"example.com"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if realIP != "" {
		req.Header.Set("X-Real-IP", realIP)
	}
	if xff != "" {
		req.Header.Set("X-Forwarded-For", xff)
	}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("POST формы: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode
}

// fakeLiveCheck — шов живой проверки: всегда успешный результат.
func fakeLiveCheck() checker.LiveFunc {
	return func(context.Context, *config.Config, checker.Pool, string, checker.Logger) (*checker.Result, error) {
		return &checker.Result{Domain: "example.com", Found: true, Score: 0.5}, nil
	}
}

// serveOn — прямой вызов хендлера сервера с заданным RemoteAddr.
func serveOn(srv *Server, remote, body string) int {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/check-domain", strings.NewReader(body))
	req.RemoteAddr = remote
	srv.Handler().ServeHTTP(rec, req)
	return rec.Code
}

// TestCheckDomainUnixSocketBuckets — критерий T-221: на unix-сокете два
// разных X-Real-IP получают независимые бакеты (первый исчерпал лимит —
// второй проходит); X-Forwarded-For без X-Real-IP работает так же;
// «@»-пир без заголовков — общий бакет-метка unixLocalKey, INSERT
// web_checks для него не выполняется вовсе (inet NOT NULL), пропуск
// виден Warn-ом в журнале сервиса.
func TestCheckDomainUnixSocketBuckets(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	db := &fakeDB{seed: seedLanding()}
	cfg := config.Default()
	cfg.Web.RateLimit = 1 // один запрос на бакет: независимость видна сразу
	srv, err := New(db, cfg, Options{Log: log})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	srv.liveCheck = fakeLiveCheck()
	client := newUnixTestServer(t, srv.Handler())

	// Независимые бакеты по X-Real-IP: первый IP исчерпал лимит —
	// второй проходит (до T-221 пир «@» не был доверенным и все
	// посетители делили один бакет: 4 проверки/мин на весь сайт).
	if st := postFormUnix(t, client, "198.51.100.1", ""); st != http.StatusOK {
		t.Fatalf("первый запрос c X-Real-IP A: %d, хочу 200", st)
	}
	if st := postFormUnix(t, client, "198.51.100.1", ""); st != http.StatusTooManyRequests {
		t.Fatalf("повтор с X-Real-IP A: %d, хочу 429 (лимит 1 исчерпан)", st)
	}
	if st := postFormUnix(t, client, "198.51.100.2", ""); st != http.StatusOK {
		t.Fatalf("запрос с X-Real-IP B: %d, хочу 200 (бакет независим)", st)
	}
	// X-Forwarded-For без X-Real-IP — запасной источник адреса: ключ —
	// ПОСЛЕДНЯЯ позиция (её дописал наш прокси; клиентское начало
	// списка не доверено — P1-1 ревью T-221).
	if st := postFormUnix(t, client, "", "6.6.6.6, 198.51.100.3"); st != http.StatusOK {
		t.Fatalf("запрос с X-Forwarded-For C: %d, хочу 200 (свой бакет по последней позиции)", st)
	}
	// Подделка первой позиции XFF не даёт новый бакет: тот же адрес
	// в конце списка — лимит исчерпан.
	if st := postFormUnix(t, client, "", "1.2.3.4, 198.51.100.3"); st != http.StatusTooManyRequests {
		t.Fatalf("подделка начала XFF: %d, хочу 429 (бакет один — по последней позиции)", st)
	}
	if got := db.webCheckInserts(); got != 3 {
		t.Fatalf("INSERT web_checks выполнен %d раз, хочу 3 (по одному на валидный IP)", got)
	}

	// «@»-пир без заголовков: общий бакет-метка — первый проходит,
	// второй исчерпывает; запись в БД не пытается (inet NOT NULL),
	// пропуск каждого запроса — Warn в журнале.
	warnsBefore := strings.Count(buf.String(), "web_checks")
	if st := postFormUnix(t, client, "", ""); st != http.StatusOK {
		t.Fatalf("первый запрос без заголовков: %d, хочу 200 (бакет-метка свеж)", st)
	}
	if st := postFormUnix(t, client, "", ""); st != http.StatusTooManyRequests {
		t.Fatalf("второй запрос без заголовков: %d, хочу 429 (общий бакет-метка)", st)
	}
	if got := db.webCheckInserts(); got != 3 {
		t.Fatalf("INSERT web_checks после не-IP пира: %d обращений, хочу по-прежнему 3 (запись не пытается)", got)
	}
	// Пропуск журнала — Warn на каждый состоявшийся запрос (первый
	// прошёл лимит и дошёл до журнала; второй — 429, до журнала не дошёл).
	if got, want := strings.Count(buf.String(), "web_checks")-warnsBefore, 1; got != want {
		t.Errorf("Warn-ов пропуска журнала %d, хочу %d (не молча):\n%s", got, want, buf.String())
	}
}

// TestCheckDomainUnixJournalError — отказ INSERT web_checks (unix-пир с
// валидным X-Real-IP): ответ посетителю не ломается, отказ логируется
// Warn-ом (до T-221 уходил через Query без чтения строк — молча).
func TestCheckDomainUnixJournalError(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	db := &fakeDB{seed: seedLanding(), execErr: errors.New("inet violation")}
	cfg := config.Default()
	cfg.Web.RateLimit = 10
	srv, err := New(db, cfg, Options{Log: log})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	srv.liveCheck = fakeLiveCheck()
	client := newUnixTestServer(t, srv.Handler())

	if st := postFormUnix(t, client, "198.51.100.7", ""); st != http.StatusOK {
		t.Fatalf("ответ при отказе журнала: %d, хочу 200 (best-effort)", st)
	}
	if !strings.Contains(buf.String(), "web_checks") {
		t.Errorf("отказ INSERT web_checks не логируется:\n%s", buf.String())
	}
	if got := db.webCheckInserts(); got != 1 {
		t.Fatalf("попыток INSERT web_checks %d, хочу 1", got)
	}
}

// TestRecordCheckClientCanceled — ушедший посетитель (отмена контекста
// запроса после живой проверки) не выглядит отказом журнала: INSERT
// web_checks живёт собственным окном без отмены клиента (P3-5 ревью
// T-221) — аудит «кто/что/когда проверял» не зависит от отключения.
func TestRecordCheckClientCanceled(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	db := &fakeDB{seed: seedLanding()}
	srv, err := New(db, config.Default(), Options{Log: log})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	srv.recordCheck(ctx, "example.com", true, 0.5, 10, "198.51.100.1")

	if got := db.webCheckInserts(); got != 1 {
		t.Fatalf("INSERT web_checks при отменённом контексте клиента: %d, хочу 1 (журнал живёт своим окном)", got)
	}
	if buf.Len() != 0 {
		t.Errorf("отмена клиента отражена как отказ журнала (не должна):\n%s", buf.String())
	}
}

// counterByLabels — значение счётчика name с точным набором меток,
// кроме сервисной; -1, если ряд не найден (стиль retentionmetrics_test).
func counterByLabels(t *testing.T, g prometheus.Gatherer, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := g.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
	nextMetric:
		for _, m := range mf.GetMetric() {
			got := make(map[string]string)
			for _, lp := range m.GetLabel() {
				if lp.GetName() == metrics.LabelService {
					continue
				}
				got[lp.GetName()] = lp.GetValue()
			}
			for k, v := range labels {
				if got[k] != v {
					continue nextMetric
				}
			}
			if c := m.GetCounter(); c != nil {
				return c.GetValue()
			}
		}
	}
	return -1
}

// TestCheckDomainMetrics — метрики формы (T-221) регистрируются в
// реестре сервиса и отдаются /metrics (:9107): исходы запросов по
// result (ok/rate_limited/invalid/failed/busy), сбои по стадиям
// (live/journal/non_ip), длительность — гистограммой.
func TestCheckDomainMetrics(t *testing.T) {
	reg := metrics.New("web")
	db := &fakeDB{seed: seedLanding(), execErr: errors.New("insert failed")}
	cfg := config.Default()
	cfg.Web.RateLimit = 1
	cfg.Web.LiveConcurrency = 1
	srv, err := New(db, cfg, Options{Metrics: reg})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	srv.liveCheck = fakeLiveCheck()

	// ok + отказ журнала (journal): первый запрос IP A.
	if st := serveOn(srv, "203.0.113.1:1000", `{"domain":"example.com"}`); st != http.StatusOK {
		t.Fatalf("ok-запрос: %d, хочу 200", st)
	}
	// rate_limited: повтор с того же IP A.
	if st := serveOn(srv, "203.0.113.1:1000", `{"domain":"example.com"}`); st != http.StatusTooManyRequests {
		t.Fatalf("повтор: %d, хочу 429", st)
	}
	// invalid: мусорный домен с IP B (валидация после лимита).
	if st := serveOn(srv, "203.0.113.2:1000", `{"domain":"not a domain"}`); st != http.StatusUnprocessableEntity {
		t.Fatalf("мусорный ввод: %d, хочу 422", st)
	}

	// busy: слот семафора занят блокированным конвейером (IP C),
	// запрос с IP D — быстрый 503; освобождение — занявший отвечает 200.
	block := make(chan struct{})
	entered := make(chan struct{}, 1)
	done := make(chan int, 1)
	srv.liveCheck = func(context.Context, *config.Config, checker.Pool, string, checker.Logger) (*checker.Result, error) {
		entered <- struct{}{}
		<-block
		return &checker.Result{Domain: "example.com"}, nil
	}
	go func() { done <- serveOn(srv, "203.0.113.4:1000", `{"domain":"example.com"}`) }()
	<-entered
	if st := serveOn(srv, "203.0.113.5:1000", `{"domain":"example.com"}`); st != http.StatusServiceUnavailable {
		t.Fatalf("запрос поверх занятого слота: %d, хочу 503 busy", st)
	}
	close(block)
	if st := <-done; st != http.StatusOK {
		t.Fatalf("занявший слот: %d, хочу 200", st)
	}

	// failed + live-ошибка: сбой конвейера без stored-данных (TCP-пир),
	// затем пир unix-сокета без заголовков — тот же исход плюс non_ip.
	failing, err := New(db, cfg, Options{Metrics: reg})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	failing.liveCheck = func(context.Context, *config.Config, checker.Pool, string, checker.Logger) (*checker.Result, error) {
		return nil, errors.New("конвейер недоступен")
	}
	if st := serveOn(failing, "203.0.113.3:1000", `{"domain":"example.com"}`); st != http.StatusBadGateway {
		t.Fatalf("сбой конвейера: %d, хочу 502", st)
	}
	if st := serveOn(failing, "@", `{"domain":"example.com"}`); st != http.StatusBadGateway {
		t.Fatalf("unix-пир без заголовков: %d, хочу 502 (сбой конвейера, stored нет)", st)
	}

	// stored_only: живая проверка сбоила, домен известен конвейеру —
	// 200 из сохранённых данных (P3-3 ревью T-221).
	seed := seedLanding()
	seed.domainInfo = []byte(`{"domain":{"status":0,"status_name":"new","last_checked_at":"2026-11-01"}}`)
	stored, err := New(&fakeDB{seed: seed}, cfg, Options{Metrics: reg})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	stored.liveCheck = failing.liveCheck
	if st := serveOn(stored, "203.0.113.6:1000", `{"domain":"example.com"}`); st != http.StatusOK {
		t.Fatalf("сбой конвейера с сохранёнными данными: %d, хочу 200 (stored_only)", st)
	}

	for _, tc := range []struct {
		name string
		got  float64
		want float64
	}{
		{"checks result=ok", counterByLabels(t, reg, "crawler_web_checks_total", map[string]string{"result": "ok"}), 2},
		{"checks result=rate_limited", counterByLabels(t, reg, "crawler_web_checks_total", map[string]string{"result": "rate_limited"}), 1},
		{"checks result=invalid", counterByLabels(t, reg, "crawler_web_checks_total", map[string]string{"result": "invalid"}), 1},
		{"checks result=busy", counterByLabels(t, reg, "crawler_web_checks_total", map[string]string{"result": "busy"}), 1},
		{"checks result=failed", counterByLabels(t, reg, "crawler_web_checks_total", map[string]string{"result": "failed"}), 2},
		{"checks result=stored_only", counterByLabels(t, reg, "crawler_web_checks_total", map[string]string{"result": "stored_only"}), 1},
		{"errors type=journal", counterByLabels(t, reg, "crawler_web_check_errors_total", map[string]string{"type": "journal"}), 3},
		{"errors type=live", counterByLabels(t, reg, "crawler_web_check_errors_total", map[string]string{"type": "live"}), 3},
		{"errors type=non_ip", counterByLabels(t, reg, "crawler_web_check_errors_total", map[string]string{"type": "non_ip"}), 1},
	} {
		if tc.got != tc.want {
			t.Errorf("%s = %v, хочу %v", tc.name, tc.got, tc.want)
		}
	}

	// Длительность — гистограмма: наблюдения есть у всех, кто дошёл до
	// живой проверки (2 ok + 2 failed + 1 stored_only), 429/422/busy не
	// наблюдались.
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	histogramObserved := false
	for _, mf := range families {
		if mf.GetName() != "crawler_web_check_seconds" {
			continue
		}
		for _, m := range mf.GetMetric() {
			if h := m.GetHistogram(); h != nil && h.GetSampleCount() == 5 {
				histogramObserved = true
			}
		}
	}
	if !histogramObserved {
		t.Error("crawler_web_check_seconds без 5 наблюдений (2 ok + 2 failed + 1 stored_only)")
	}

	// Отдача реестра хендлером /metrics (как каркас на :9107).
	rec := httptest.NewRecorder()
	metrics.Handler(reg).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{
		"crawler_web_checks_total", "crawler_web_check_errors_total", "crawler_web_check_seconds",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics не отдаёт %s", want)
		}
	}
}

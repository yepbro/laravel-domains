// Интеграционные тесты HTTP-этапа на тестовом стенде (T-060; ТЗ 11.2, §8,
// 5.5): компоненты этапа 5 против живого стека testweb (Caddy) и mockserver
// («злые» ответы, виртуальные хосты сценариев scenarios.yaml).
//
// Уровень — компоненты конвейера, не сырой протокол: TestHTTP_TestwebScenarios
// и TestHTTP_MockserverEvil (env_test.go) уже зафиксировали фикстуры и
// эндпоинты обменами net/http; здесь те же сценарии прогоняются через
// транспорт httpfetch (лимиты и анти-тарпит T-049, цепочки редиректов T-048,
// TLS-атрибуты T-050, распаковка с потолком T-051) и воркер httpworker
// (ProcessDomain + ContentSaver T-056 + Recorder T-058 — классификация
// ошибок §8, варианты/алиасы T-054/T-055, SPA-вердикт T-057).
//
// Сквозной минимум (как T-040 для DNS): задачи q:http по сценариям same-site
// и bare→www через живые PostgreSQL/Redis: строки subdomain_results с
// вариантами и алиасами, публикация q:analyze, refcount contents при
// повторной доставке. Изоляция — схема migratedSchema и Redis БД 15
// (resilienceRedisEnv) с очисткой q:* до/после.
//
// Время тестов — секунды: транспортные таймауты переопределены короткими
// значениями (таблица лимитов ТЗ 5.5 настраивается), ответы стенда —
// миллисекунды; медленные ветки (тарпит ~5 с, чёрная дыра ~3 с) — осознанные
// ожидания классификации, а не фоновые задержки.
package integration

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"crawler/internal/config"
	"crawler/internal/content"
	"crawler/internal/httpfetch"
	"crawler/internal/httpworker"
	"crawler/internal/model"
	"crawler/internal/queue"
)

// httpBlackholeIP — адрес чёрной дыры стенда (closed-ports): TCP-диал
// завершается EHOSTUNREACH за ~3 с (ARP без ответа) — детерминированная
// ошибка сети, класс ErrNetwork → no_http.
const httpBlackholeIP = "172.28.100.250"

// httpTarpitSpeed/httpTarpitCap — параметры анти-тарпита для
// TestHTTPFetch_Tarpit: скорость 8192 байт/с против отдачи mockserver
// 512 байт/с и потолок 4 КиБ — сторож срабатывает за ~1 с (дедлайн
// remain/speed = 500 мс < интервал порций 1 с), общий таймаут (20 с)
// не успевает: проверяется именно механизм T-049.
const (
	httpTarpitSpeed = int64(8192)
	httpTarpitCap   = int64(4096)
)

// httpFetchURL парсит URL сценария (ошибка — фатальна, имена фиксированы).
func httpFetchURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("URL %s: %v", raw, err)
	}
	return u
}

// httpTestFetcher — транспорт этапа с InsecureSkipVerify (ТЗ 5.5 п. 3) и
// честным UA (§9) поверх тестовых параметров: mutate правит Options до
// создания (таймауты, лимиты, резолвер). nil-мутация — дефолты таблицы
// лимитов ТЗ 5.5.
func httpTestFetcher(t *testing.T, mutate func(*httpfetch.Options)) *httpfetch.Fetcher {
	t.Helper()
	opts := httpfetch.Options{
		UserAgent:       config.DefaultUserAgent,
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // testbed, ТЗ 5.5 п. 3
		Limits:          &httpfetch.LimitOptions{},
	}
	if mutate != nil {
		mutate(&opts)
	}
	f := httpfetch.NewFetcher(opts)
	t.Cleanup(f.CloseIdleConnections)
	return f
}

// ---------------------------------------------------------------- транспорт

// TestHTTPFetch_Tarpit — анти-тарпит T-049 (ТЗ 5.5 «мин. скорость после
// первого байта», §8): mockserver отдаёт тело со скоростью 512 байт/с —
// ниже минимальной; сторож закрывает соединение, чтение возвращается
// ошибкой с тайм-классификацией (temp_error у потребителя, не «нет сайта»).
func TestHTTPFetch_Tarpit(t *testing.T) {
	skipUnlessIntegration(t)
	f := httpTestFetcher(t, func(o *httpfetch.Options) {
		o.MaxBodyBytes = httpTarpitCap
		o.Limits.MaxBodyBytes = httpTarpitCap
		o.Limits.MinBodySpeed = httpTarpitSpeed
	})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	res, err := f.Fetch(ctx, httpfetch.FetchRequest{
		URL:   httpFetchURL(t, "http://tarpit.crawl.test/"),
		Addrs: []netip.Addr{netip.MustParseAddr(mockserverIP)},
	})
	if err != nil {
		t.Fatalf("Fetch (заголовки тарпита приходят сразу): %v", err)
	}
	if res.StatusCode != 200 {
		t.Fatalf("статус = %d, want 200", res.StatusCode)
	}

	start := time.Now()
	body, readErr := io.ReadAll(res.Body)
	_ = res.Body.Close()
	if readErr == nil {
		t.Fatal("чтение бесконечного медленного тела должно прерваться сторожем скорости")
	}
	if !strings.Contains(readErr.Error(), "тарпит") {
		t.Errorf("ошибка не анти-тарпит: %v", readErr)
	}
	if !httpfetch.IsTimeout(readErr) {
		t.Errorf("ошибка без тайм-классификации (temp_error у воркера): %v", readErr)
	}
	if n := len(body); n == 0 || int64(n) > httpTarpitCap {
		t.Errorf("прочитано %d байт — вне диапазона (0; %d]", n, httpTarpitCap)
	}
	if el := time.Since(start); el > 10*time.Second {
		t.Errorf("срабатывание заняло %v — работал общий таймаут, а не сторож скорости", el)
	}
}

// TestHTTPFetch_HugeBody — потолок тела T-049 (ТЗ 5.5 «макс. размер
// читаемого тела | 2 МБ», §8): mockserver заявляет Content-Length 1 ГиБ и
// реально льёт сверх лимита — тело обрезается ровно на 2 МиБ, признак
// Truncated, чтение без ошибки сети (данные частичны, не сбой).
func TestHTTPFetch_HugeBody(t *testing.T) {
	skipUnlessIntegration(t)
	f := httpTestFetcher(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	res, err := f.Fetch(ctx, httpfetch.FetchRequest{
		URL:   httpFetchURL(t, "http://huge-body.crawl.test/"),
		Addrs: []netip.Addr{netip.MustParseAddr(mockserverIP)},
	})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	defer func() { _ = res.Body.Close() }()
	if got := res.Header.Get("Content-Length"); got != "1073741824" {
		t.Fatalf("Content-Length = %q, want 1073741824 (сценарий заявляет гигабайт)", got)
	}

	body, readErr := io.ReadAll(res.Body)
	if readErr != nil {
		t.Fatalf("обрезка потолком — не ошибка сети: %v", readErr)
	}
	if n := len(body); n != 2<<20 {
		t.Errorf("прочитано %d байт, want ровно %d (потолок)", n, 2<<20)
	}
	if res.BodyLimiter == nil || !res.BodyLimiter.Truncated() {
		t.Error("признак Truncated не выставлен (решение о частичности — за T-056/T-058)")
	}
}

// TestHTTPFetch_ZipBomb — потолок распаковки T-051 (ТЗ 5.5 «распаковка — с
// ограничением размера», §8): бесконечный gzip-поток нулей распаковывается
// не дальше MaxBodyBytes — ErrDecompressTooLarge. br-сценарий отсутствует:
// mockserver отдаёт только gzip, testweb (Caddy) ответы не кодирует.
func TestHTTPFetch_ZipBomb(t *testing.T) {
	skipUnlessIntegration(t)
	f := httpTestFetcher(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	res, err := f.Fetch(ctx, httpfetch.FetchRequest{
		URL:   httpFetchURL(t, "http://zip-bomb.crawl.test/"),
		Addrs: []netip.Addr{netip.MustParseAddr(mockserverIP)},
	})
	if err != nil {
		t.Fatalf("Fetch (заголовки приходят, ошибка — в распаковке тела): %v", err)
	}
	_, readErr := io.ReadAll(res.Body)
	_ = res.Body.Close()
	if !errors.Is(readErr, httpfetch.ErrDecompressTooLarge) {
		t.Fatalf("ошибка чтения = %v, want ErrDecompressTooLarge", readErr)
	}
}

// TestHTTPChain_RedirectLoops — цепочки T-048 (ТЗ 5.5 п. 1, §8):
// бесконечный редирект останавливается лимитом max_redirects, кольцо —
// детектом цикла; обе причины — единая классификация ErrTooManyRedirects,
// НЕ temp_error (постоянное свойство сайта, ТЗ 6.2).
func TestHTTPChain_RedirectLoops(t *testing.T) {
	skipUnlessIntegration(t)
	f := httpTestFetcher(t, nil)
	addrs := []netip.Addr{netip.MustParseAddr(mockserverIP)}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	t.Run("бесконечный редирект — лимит", func(t *testing.T) {
		cr, err := httpfetch.FetchChain(ctx, f, httpfetch.FetchRequest{
			URL:   httpFetchURL(t, "http://infinite-redirect.crawl.test/"),
			Addrs: addrs,
		})
		if !errors.Is(err, httpfetch.ErrTooManyRedirects) {
			t.Fatalf("ошибка = %v, want ErrTooManyRedirects", err)
		}
		if !strings.Contains(err.Error(), "лимит") {
			t.Errorf("причина не лимит: %v", err)
		}
		if httpfetch.IsTimeout(err) {
			t.Error("редирект-лимит не тайм-класс (не temp_error, ТЗ 6.2)")
		}
		if cr == nil || cr.Result != nil {
			t.Fatal("итоговой страницы нет: Result обязан быть nil")
		}
		if want := httpfetch.DefaultMaxRedirects + 1; len(cr.Chain) != want {
			t.Errorf("цепочка = %d шагов, want %d (старт + 8 переходов)", len(cr.Chain), want)
		}
	})

	t.Run("циклический редирект — детект цикла", func(t *testing.T) {
		cr, err := httpfetch.FetchChain(ctx, f, httpfetch.FetchRequest{
			URL:   httpFetchURL(t, "http://cycle-redirect.crawl.test/"),
			Addrs: addrs,
		})
		if !errors.Is(err, httpfetch.ErrTooManyRedirects) {
			t.Fatalf("ошибка = %v, want ErrTooManyRedirects (цикл — та же классификация)", err)
		}
		if !strings.Contains(err.Error(), "цикл") {
			t.Errorf("причина не цикл: %v", err)
		}
		if cr == nil || cr.Result != nil || len(cr.Chain) != 3 {
			t.Errorf("итог = %+v (chain %d), want Result nil и цепочка «/»→/b→/a (старт — главная страница сценария)", cr, len(cr.Chain))
		}
	})
}

// TestHTTPFetch_BadTLS — некорректные сертификаты (T-050; ТЗ 5.5 п. 3, §8):
// коннект проходит (InsecureSkipVerify — нужен контент), атрибуты
// сертификата сохранены, самостоятельная верификация даёт Valid=false с
// типизированными причинами. Домены сценариев (tls-*.crawl.test, SNI) и
// отладочные bad-tls-имена из env_test.go несут одно и то же свойство.
func TestHTTPFetch_BadTLS(t *testing.T) {
	skipUnlessIntegration(t)
	f := httpTestFetcher(t, nil)
	addrs := []netip.Addr{netip.MustParseAddr(mockserverIP)}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	cases := []struct {
		host     string
		problems []httpfetch.TLSProblem
	}{
		{"tls-self-signed.crawl.test", []httpfetch.TLSProblem{httpfetch.TLSProblemSelfSigned}},
		{"tls-expired.crawl.test", []httpfetch.TLSProblem{httpfetch.TLSProblemExpired, httpfetch.TLSProblemSelfSigned}},
		{"tls-wrong-san.crawl.test", []httpfetch.TLSProblem{httpfetch.TLSProblemHostnameMismatch, httpfetch.TLSProblemSelfSigned}},
		{"self-signed.bad-tls.crawl.test", []httpfetch.TLSProblem{httpfetch.TLSProblemSelfSigned}},
		{"expired.bad-tls.crawl.test", []httpfetch.TLSProblem{httpfetch.TLSProblemExpired, httpfetch.TLSProblemSelfSigned}},
		{"wrong-san.bad-tls.crawl.test", []httpfetch.TLSProblem{httpfetch.TLSProblemHostnameMismatch, httpfetch.TLSProblemSelfSigned}},
	}
	for _, tc := range cases {
		t.Run(tc.host, func(t *testing.T) {
			res, err := f.Fetch(ctx, httpfetch.FetchRequest{
				URL:   httpFetchURL(t, "https://"+tc.host+"/"),
				Addrs: addrs,
			})
			if err != nil {
				t.Fatalf("коннект с InsecureSkipVerify обязан проходить: %v", err)
			}
			defer func() { _ = res.Body.Close() }()
			if res.StatusCode != 200 {
				t.Fatalf("статус = %d, want 200 (некорректный сертификат загрузке не мешает, §8)", res.StatusCode)
			}
			info := res.TLSInfo
			if info == nil {
				t.Fatal("TLSInfo не разобран (издатель/срок/SAN обязаны сохраняться)")
			}
			if info.Valid {
				t.Error("Valid = true у некорректного сертификата")
			}
			if len(info.Problems) != len(tc.problems) {
				t.Fatalf("причины = %v, want %v", info.Problems, tc.problems)
			}
			for i, p := range tc.problems {
				if info.Problems[i] != p {
					t.Errorf("причина[%d] = %s, want %s (порядок: срок, хост, самоподписанность)", i, info.Problems[i], p)
				}
			}
			if !info.SelfSigned {
				t.Error("SelfSigned = false — сертификаты mockserver самодельные")
			}
		})
	}

	t.Run("http — TLSInfo nil, не ошибка", func(t *testing.T) {
		res, err := f.Fetch(ctx, httpfetch.FetchRequest{
			URL:   httpFetchURL(t, "http://cookies.crawl.test/"),
			Addrs: addrs,
		})
		if err != nil {
			t.Fatalf("Fetch: %v", err)
		}
		defer func() { _ = res.Body.Close() }()
		if res.TLSInfo != nil {
			t.Error("TLSInfo у http-ответа должен быть nil (§8)")
		}
	})
}

// ---------------------------------------------------------------- конвейер

// httpWorkerEnv — конвейер воркера на живых PostgreSQL/Redis: изолированная
// схема, очередь (публикация q:analyze), PackStore во временном каталоге и
// цепочка OnResult как в сборке cmd/http-worker (T-056 → T-058). fetcher —
// транспорт этапа (адаптируется тестом: таймауты, резолв).
type httpWorkerEnv struct {
	pool     *pgxpool.Pool
	rdb      *redis.Client
	q        *queue.RedisQueue
	store    *content.PackStore
	saver    *httpworker.ContentSaver
	recorder *httpworker.Recorder
	worker   *httpworker.Worker
}

// httpWorkerSetup собирает env: mutate правит параметры транспорта.
func httpWorkerSetup(t *testing.T, mutate func(*httpfetch.Options)) *httpWorkerEnv {
	t.Helper()
	pool := migratedSchema(t)
	rdb := resilienceRedisEnv(t)
	q := queue.NewRedisQueue(rdb, queue.RedisOptions{Block: 150 * time.Millisecond})

	store, err := content.OpenPackStore(pool, content.PackOptions{
		Dir:           t.TempDir(),
		PackSize:      8 << 10,
		MaxStoreBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("OpenPackStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	saver, err := httpworker.NewContentSaver(store, nil)
	if err != nil {
		t.Fatal(err)
	}
	recorder, err := httpworker.NewRecorder(pool, q, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Резолв хостов редиректов — тем же резолвером DNS-этапа (ТЗ 5.5 п. 2,
	// адаптер httpworker.DNSLookup + кэш, как в сборке cmd/http-worker).
	resolve := dnsEnv(t)
	fetcher := httpTestFetcher(t, func(o *httpfetch.Options) {
		o.Resolver = httpfetch.NewCachingResolver(
			httpworker.NewDNSLookup(resolve, false), httpfetch.CacheOptions{})
		if mutate != nil {
			mutate(o)
		}
	})

	// MaxRedirects — обязательный явный лимит: 0 в Options означает
	// «не следовать» (T-053), а не дефолт.
	w, err := httpworker.New(httpworker.ChainFetch(fetcher), httpworker.Options{
		MaxRedirects: httpfetch.DefaultMaxRedirects,
		Store:        store,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &httpWorkerEnv{
		pool: pool, rdb: rdb, q: q, store: store,
		saver: saver, recorder: recorder, worker: w,
	}
}

// process — эпизод домена целиком, как в processDelivery воркера:
// ProcessDomain → сохранение контента (T-056) → запись результата и
// публикация q:analyze (T-058). Возвращает результат с уже применённой
// разметкой алиасов и нормализацией ContentSaver-а.
func (e *httpWorkerEnv) process(ctx context.Context, t *testing.T, task queue.HTTPTask) *httpworker.DomainResult {
	t.Helper()
	res, err := e.worker.ProcessDomain(ctx, task)
	if err != nil {
		t.Fatalf("ProcessDomain %s: %v", task.Name, err)
	}
	if err := e.saver.Save(ctx, res); err != nil {
		t.Fatalf("ContentSaver %s: %v", task.Name, err)
	}
	if _, err := e.recorder.RecordAndPublish(ctx, res); err != nil {
		t.Fatalf("RecordAndPublish %s: %v", task.Name, err)
	}
	return res
}

// httpTask — задача q:http на домен с субдоменами «метка → адрес».
// Порядок субдоменов детерминирован (по возрастанию id), как в задачах
// DNS-этапа (T-039).
func httpTask(zoneID int16, domainID int64, name string, subs map[int16]string) queue.HTTPTask {
	task := queue.HTTPTask{ZoneID: zoneID, DomainID: domainID, Name: name}
	for _, id := range []int16{0, 1} {
		label, ok := subs[id]
		if !ok {
			continue
		}
		task.Subdomains = append(task.Subdomains, queue.HTTPSubdomain{
			SubdomainID: id, Label: label, Addrs: []string{httpScenarioAddr(name, label)},
		})
	}
	return task
}

// httpScenarioAddr — адрес стенда для субдомена сценария: testweb для
// фикстур Caddy, mockserver для «злых» ответов, чёрная дыра для
// closed-ports (маршрутизация адресов — та же, что в зонах CoreDNS).
func httpScenarioAddr(name, label string) string {
	host := name
	if label != "" {
		host = label + "." + name
	}
	switch {
	case strings.HasPrefix(host, "closed-ports."):
		return httpBlackholeIP
	case strings.HasPrefix(host, "spa-shell."), strings.HasPrefix(host, "same-site."),
		strings.HasPrefix(host, "www.same-site."), strings.HasPrefix(host, "bare-to-www."),
		strings.HasPrefix(host, "www.bare-to-www."):
		return testwebIP
	default:
		return mockserverIP
	}
}

// httpSubdomainRow — строка subdomain_results для проверок.
type httpSubdomainRow struct {
	subdomainID    int16
	variant        int16
	scheme         int16
	httpStatus     int16
	finalURL       string
	aliasKind      int16
	aliasSubdomain *int16
	contentHash    []byte
	errCode        *int16
}

// httpRows читает строки subdomain_results домена, отсортированные по
// subdomain_id (порядок вставки не специфицирован).
func httpRows(t *testing.T, pool *pgxpool.Pool, domainID int64) []httpSubdomainRow {
	t.Helper()
	rows, err := pool.Query(context.Background(),
		`SELECT subdomain_id, variant, scheme, http_status, final_url, alias_kind,
		        alias_subdomain_id, content_hash, error
		 FROM subdomain_results WHERE domain_id = $1 ORDER BY subdomain_id`, domainID)
	if err != nil {
		t.Fatalf("чтение subdomain_results: %v", err)
	}
	defer rows.Close()
	var out []httpSubdomainRow
	for rows.Next() {
		var r httpSubdomainRow
		if err := rows.Scan(&r.subdomainID, &r.variant, &r.scheme, &r.httpStatus, &r.finalURL,
			&r.aliasKind, &r.aliasSubdomain, &r.contentHash, &r.errCode); err != nil {
			t.Fatalf("строка subdomain_results: %v", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("итерация subdomain_results: %v", err)
	}
	return out
}

// httpDomainRow — состояние домена после эпизода.
type httpDomainRow struct {
	status        model.Status
	checkState    int16
	queuedAt      *time.Time
	lastCheckedAt *time.Time
	tempAttempts  int16
}

func httpDomainState(t *testing.T, pool *pgxpool.Pool, domainID int64) httpDomainRow {
	t.Helper()
	var r httpDomainRow
	err := pool.QueryRow(context.Background(),
		`SELECT status, check_state, queued_at, last_checked_at, temp_attempts
		 FROM domains WHERE id = $1`, domainID,
	).Scan(&r.status, &r.checkState, &r.queuedAt, &r.lastCheckedAt, &r.tempAttempts)
	if err != nil {
		t.Fatalf("чтение domains: %v", err)
	}
	return r
}

// TestHTTPWorker_E2E — сквозной минимум HTTP-этапа (ТЗ 5.5 пп. 5–6, 9, §7;
// как T-040 для DNS): задачи q:http по сценариям same-site (http→https на
// один сайт — единственная запись variant 0) и bare→www (редирект на
// субдомен — alias_kind 2, контент хранится один раз) через живые
// PostgreSQL/Redis: строки subdomain_results, публикация q:analyze,
// refcount contents при повторной доставке (at-least-once, ТЗ 3.4).
func TestHTTPWorker_E2E(t *testing.T) {
	skipUnlessIntegration(t)
	env := httpWorkerSetup(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	labels := []string{"same-site", "bare-to-www"}
	ids := insertPendingDomains(t, env.pool, "crawl.test", labels)
	var zoneID int16
	if err := env.pool.QueryRow(ctx,
		"SELECT id FROM zones WHERE name = $1", "crawl.test").Scan(&zoneID); err != nil {
		t.Fatalf("зона crawl.test: %v", err)
	}
	// Хэш страницы bare-to-www для проверок contents.
	var pageHash *content.Hash

	t.Run("same-site: один сайт — единственная запись variant 0", func(t *testing.T) {
		task := httpTask(zoneID, ids[0], "same-site.crawl.test", map[int16]string{0: "", 1: "www"})
		res := env.process(ctx, t, task)
		if !res.HasRecords() {
			t.Fatal("домен same-site обязан быть alive (записи есть)")
		}

		apex := res.Subdomains[0]
		if len(apex.Records) != 1 {
			t.Fatalf("записей апекса: %d, want 1 (протоколы сходятся на один сайт)", len(apex.Records))
		}
		rec := apex.Records[0]
		if rec.Variant != model.VariantMain || rec.Scheme != model.SchemeHTTPS {
			t.Errorf("вариант/схема = %d/%d, want 0/https (https канонический, ТЗ 5.5 п. 5)", rec.Variant, rec.Scheme)
		}
		if rec.FinalURL != "https://same-site.crawl.test/" {
			t.Errorf("итоговый URL = %q, want https://same-site.crawl.test/", rec.FinalURL)
		}
		if rec.AliasKind != model.AliasNone || rec.AliasSubdomainID != nil {
			t.Errorf("алиас апекса = %d/%v, want нет", rec.AliasKind, rec.AliasSubdomainID)
		}
		if rec.HTTPStatus != 200 || rec.ErrClass != httpworker.ErrNone {
			t.Errorf("статус/ошибка = %d/%s, want 200/none", rec.HTTPStatus, rec.ErrClass)
		}
		if !rec.StoreContent || rec.ContentHash == nil {
			t.Error("контент оригинала не сохраняется (StoreContent/ContentHash)")
		}

		// www.same-site не настроен в Caddy: https отклоняется по SNI, http
		// отвечает пустой страницей — одна запись со схемой своей проверки
		// («работает только один протокол», ТЗ 5.5 п. 5).
		www := res.Subdomains[1]
		if len(www.Records) != 1 || www.Records[0].Scheme != model.SchemeHTTP {
			t.Fatalf("записи www = %d, want 1 по http", len(www.Records))
		}
		if www.HTTPS.Available {
			t.Error("https www.same-site должен падать (хост не настроен в Caddy)")
		}
	})

	t.Run("bare→www: алиас и контент один раз", func(t *testing.T) {
		task := httpTask(zoneID, ids[1], "bare-to-www.crawl.test", map[int16]string{0: "", 1: "www"})
		res := env.process(ctx, t, task)

		apex := res.Subdomains[0]
		if len(apex.Records) != 1 {
			t.Fatalf("записей апекса: %d, want 1", len(apex.Records))
		}
		arec := apex.Records[0]
		if arec.AliasKind != model.AliasRedirect {
			t.Errorf("alias_kind апекса = %d, want 2 (редирект на субдомен-оригинал)", arec.AliasKind)
		}
		if arec.AliasSubdomainID == nil || *arec.AliasSubdomainID != 1 {
			t.Errorf("alias_subdomain_id = %v, want 1 (www)", arec.AliasSubdomainID)
		}
		if arec.StoreContent || arec.ContentHash != nil {
			t.Error("алиас не хранит собственный контент (хэш нормализуется в NULL)")
		}
		if arec.FinalURL != "https://www.bare-to-www.crawl.test/" {
			t.Errorf("итоговый URL апекса = %q, want https://www.bare-to-www.crawl.test/", arec.FinalURL)
		}

		www := res.Subdomains[1]
		if len(www.Records) != 1 {
			t.Fatalf("записей www: %d, want 1", len(www.Records))
		}
		wrec := www.Records[0]
		if wrec.AliasKind != model.AliasNone || wrec.AliasSubdomainID != nil {
			t.Errorf("alias www = %d/%v, want нет (запись-оригинал «дома»)", wrec.AliasKind, wrec.AliasSubdomainID)
		}
		if !wrec.StoreContent || wrec.ContentHash == nil {
			t.Fatal("контент оригинала не сохранён")
		}
		h := *wrec.ContentHash
		pageHash = &h

		// Страница читается из хранилища (roundtrip, ТЗ 11.4 п. 3).
		body, err := env.store.Get(ctx, h)
		if err != nil {
			t.Fatalf("Get контента: %v", err)
		}
		if !strings.Contains(string(body), "bare-to-www.crawl.test") {
			t.Errorf("контент не похож на страницу фикстуры: %.80q", body)
		}
	})

	t.Run("строки subdomain_results", func(t *testing.T) {
		rows := httpRows(t, env.pool, ids[1])
		if len(rows) != 2 {
			t.Fatalf("строк у bare-to-www: %d, want 2 (апекс и www, variant 0)", len(rows))
		}
		apex, www := rows[0], rows[1]
		if apex.subdomainID != 0 || www.subdomainID != 1 {
			t.Fatalf("порядок строк: subdomain_id = %d, %d, want 0, 1", apex.subdomainID, www.subdomainID)
		}
		for _, r := range rows {
			if r.variant != int16(model.VariantMain) || r.scheme != int16(model.SchemeHTTPS) {
				t.Errorf("строка %d: variant/scheme = %d/%d, want 0/https", r.subdomainID, r.variant, r.scheme)
			}
			if r.httpStatus != 200 || r.finalURL != "https://www.bare-to-www.crawl.test/" {
				t.Errorf("строка %d: статус/URL = %d/%q", r.subdomainID, r.httpStatus, r.finalURL)
			}
			if r.errCode != nil {
				t.Errorf("строка %d: error = %d, want NULL", r.subdomainID, *r.errCode)
			}
		}
		if apex.aliasKind != int16(model.AliasRedirect) || apex.aliasSubdomain == nil || *apex.aliasSubdomain != 1 {
			t.Errorf("апекс: alias = %d/%v, want 2/1", apex.aliasKind, apex.aliasSubdomain)
		}
		if apex.contentHash != nil {
			t.Error("апекс: content_hash должен быть NULL (контент — у оригинала)")
		}
		if www.aliasKind != int16(model.AliasNone) || www.aliasSubdomain != nil {
			t.Errorf("www: alias = %d/%v, want нет", www.aliasKind, www.aliasSubdomain)
		}
		if len(www.contentHash) != 16 {
			t.Errorf("www: content_hash = %d байт, want 16", len(www.contentHash))
		}
	})

	t.Run("домены: alive/done, lease завершён", func(t *testing.T) {
		for i, label := range labels {
			r := httpDomainState(t, env.pool, ids[i])
			if r.status != model.StatusAlive {
				t.Errorf("%s: status = %s, want alive", label, r.status)
			}
			if r.checkState != int16(model.CheckStateDone) {
				t.Errorf("%s: check_state = %d, want done (§7)", label, r.checkState)
			}
			if r.queuedAt != nil || r.lastCheckedAt == nil || r.tempAttempts != 0 {
				t.Errorf("%s: queued_at=%v last_checked_at=%v temp_attempts=%d", label, r.queuedAt, r.lastCheckedAt, r.tempAttempts)
			}
		}
	})

	t.Run("публикация q:analyze", func(t *testing.T) {
		if n := streamLen(t, env.rdb, queue.StreamAnalyze); n != 4 {
			t.Fatalf("записей в q:analyze = %d, want 4 (по одной на запись субдомен×вариант)", n)
		}
		got := map[string]bool{}
		for _, d := range consumeTasks(t, env.q, queue.StreamAnalyze, "it-http-e2e", "an", 10) {
			res, err := queue.Decode[queue.Result](d)
			if err != nil {
				t.Fatalf("декодирование Result: %v", err)
			}
			got[fmt.Sprintf("%d/%d/%d", res.DomainID, res.SubdomainID, res.Variant)] = true
		}
		want := map[string]bool{}
		for _, id := range ids {
			want[fmt.Sprintf("%d/0/0", id)] = true
			want[fmt.Sprintf("%d/1/0", id)] = true
		}
		if len(got) != len(want) {
			t.Fatalf("ключи q:analyze = %v, want %v", got, want)
		}
		for k := range want {
			if !got[k] {
				t.Errorf("в q:analyze нет ключа %s (домен/субдомен/вариант)", k)
			}
		}
	})

	t.Run("повторная доставка: refcount не течёт", func(t *testing.T) {
		if pageHash == nil {
			t.Fatal("хэш страницы не зафиксирован (проверка bare→www не прошла)")
		}
		before, ok := contentRowByHash(t, env.pool, *pageHash)
		if !ok {
			t.Fatal("контент страницы не записан в contents до повтора")
		}
		if before.refc != 1 {
			t.Fatalf("refcount до повтора = %d, want 1", before.refc)
		}

		task := httpTask(zoneID, ids[1], "bare-to-www.crawl.test", map[int16]string{0: "", 1: "www"})
		env.process(ctx, t, task)

		after, ok := contentRowByHash(t, env.pool, *pageHash)
		if !ok {
			t.Fatal("контент исчез после повторной доставки")
		}
		if after.refc != 1 {
			t.Errorf("refcount после повтора = %d, want 1 (повторный Put +1 и DELETE старой строки −1)", after.refc)
		}
		if n := len(httpRows(t, env.pool, ids[1])); n != 2 {
			t.Errorf("строк после повтора = %d, want 2 (перезапись, а не рост)", n)
		}
		if n := streamLen(t, env.rdb, queue.StreamAnalyze); n != 6 {
			t.Errorf("записей в q:analyze = %d, want 6 (дубликаты ограничены доставкой)", n)
		}
	})
}

// TestHTTPWorker_EvilScenarios — «злые» ответы через конвейер воркера
// (ТЗ §8, 5.5): классификация ошибок проверок и итоговые статусы доменов
// по §7 — тарпит → temp_error (тайм-класс), чёрная дыра → no_http,
// гигантское тело → обрезка без сохранения контента (T-056), zip-бомба и
// бесконечный редирект → коды ErrBody/ErrRedirects без temp-класса,
// SPA-оболочка → вердикт рендера (T-057), некорректные сертификаты →
// загрузка + TLSInfo с причиной (T-050).
func TestHTTPWorker_EvilScenarios(t *testing.T) {
	skipUnlessIntegration(t)
	// Тарпит на уровне конвейера обязан классифицироваться анти-тарпитом
	// T-049 (limitBodySpeedError — тайм-класс → temp_error): потолок тела и
	// мин. скорость ужаты, чтобы сторож сработал задолго до общего таймаута
	// (в бою то же соотношение даёт потолок 2 МиБ против скоростей ниже
	// 2 КиБ/с). Прочие сценарии ужатым потолком не затронуты: их ответы
	// короче 4 КиБ или падают по своим лимитам (распаковка, редиректы).
	env := httpWorkerSetup(t, func(o *httpfetch.Options) {
		o.TotalTimeout = 5 * time.Second
		o.MaxBodyBytes = httpTarpitCap
		o.Limits.MaxBodyBytes = httpTarpitCap
		o.Limits.MinBodySpeed = httpTarpitSpeed
	})
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	labels := []string{
		"tarpit", "closed-ports", "huge-body", "zip-bomb",
		"infinite-redirect", "spa-shell", "tls-self-signed", "tls-expired", "tls-wrong-san",
	}
	ids := insertPendingDomains(t, env.pool, "crawl.test", labels)
	var zoneID int16
	if err := env.pool.QueryRow(ctx,
		"SELECT id FROM zones WHERE name = $1", "crawl.test").Scan(&zoneID); err != nil {
		t.Fatalf("зона crawl.test: %v", err)
	}
	byLabel := map[string]int64{}
	for i, l := range labels {
		byLabel[l] = ids[i]
	}
	// Все сценарии без www: один субдомен (апекс) — так объявлены в
	// scenarios.yaml.
	run := func(label string) *httpworker.DomainResult {
		t.Helper()
		name := label + ".crawl.test"
		return env.process(ctx, t, httpTask(zoneID, byLabel[label], name, map[int16]string{0: ""}))
	}

	t.Run("тарпит: тайм-классификация → temp_error", func(t *testing.T) {
		res := run("tarpit")
		for _, c := range []struct {
			name string
			chk  httpworker.SchemeCheck
		}{{"https", res.Subdomains[0].HTTPS}, {"http", res.Subdomains[0].HTTP}} {
			if c.chk.ErrClass != httpworker.ErrTimeout {
				t.Errorf("%s: класс = %s, want timeout (temp_error, ТЗ 6.2)", c.name, c.chk.ErrClass)
			}
			if c.chk.Available {
				t.Errorf("%s: Available у оборванного тарпита", c.name)
			}
		}
		if res.HasRecords() || !res.AnyTimeout() {
			t.Fatal("записей нет и есть таймауты — вход статуса temp_error (§7)")
		}
		if r := httpDomainState(t, env.pool, byLabel["tarpit"]); r.status != model.StatusTempError {
			t.Errorf("статус тарпита = %s, want temp_error (не no_http)", r.status)
		}
	})

	t.Run("закрытые порты: без ответа → no_http", func(t *testing.T) {
		res := run("closed-ports")
		if res.HasRecords() {
			t.Fatal("веб молчит — записей быть не должно")
		}
		if res.AnyTimeout() {
			t.Error("отказ соединения — не тайм-класс (не temp_error)")
		}
		if r := httpDomainState(t, env.pool, byLabel["closed-ports"]); r.status != model.StatusNoHTTP {
			t.Errorf("статус = %s, want no_http", r.status)
		}
	})

	t.Run("гигантское тело: обрезка без контента", func(t *testing.T) {
		res := run("huge-body")
		recs := res.Subdomains[0].Records
		if len(recs) != 1 {
			t.Fatalf("записей: %d, want 1 (статус получен — протокол доступен)", len(recs))
		}
		rec := recs[0]
		if rec.HTTPStatus != 200 || !rec.Truncated || rec.ErrClass != httpworker.ErrBody {
			t.Errorf("статус/обрезка/класс = %d/%v/%s, want 200/truncated/body (§8)", rec.HTTPStatus, rec.Truncated, rec.ErrClass)
		}
		if rec.ContentHash != nil || !rec.ContentNotStored {
			t.Error("обрезанное тело не хэшируется и не сохраняется (хэш усечённых байт врал бы при дедупе, T-056)")
		}
		rows := httpRows(t, env.pool, byLabel["huge-body"])
		if len(rows) != 1 || rows[0].errCode == nil || *rows[0].errCode != int16(httpworker.ErrBody) {
			t.Fatalf("строки БД = %+v, want одна с error=body", rows)
		}
		if rows[0].contentHash != nil {
			t.Error("content_hash обрезанной записи должен быть NULL")
		}
		if r := httpDomainState(t, env.pool, byLabel["huge-body"]); r.status != model.StatusAlive {
			t.Errorf("статус = %s, want alive (страница ответила)", r.status)
		}
	})

	t.Run("zip-бомба: ErrBody без записей", func(t *testing.T) {
		res := run("zip-bomb")
		for _, chk := range []httpworker.SchemeCheck{res.Subdomains[0].HTTPS, res.Subdomains[0].HTTP} {
			if chk.ErrClass != httpworker.ErrBody || chk.Available {
				t.Errorf("класс/Available = %s/%v, want body/false (потолок распаковки, T-051)", chk.ErrClass, chk.Available)
			}
		}
		if res.HasRecords() {
			t.Error("страницы нет — записей быть не должно")
		}
		if r := httpDomainState(t, env.pool, byLabel["zip-bomb"]); r.status != model.StatusNoHTTP {
			t.Errorf("статус = %s, want no_http (лимит — не temp-класс)", r.status)
		}
	})

	t.Run("бесконечный редирект: ErrRedirects, не temp", func(t *testing.T) {
		res := run("infinite-redirect")
		chk := res.Subdomains[0].HTTPS
		if chk.ErrClass != httpworker.ErrRedirects {
			t.Fatalf("класс = %s, want redirects", chk.ErrClass)
		}
		if chk.ErrClass.IsTemp() {
			t.Error("редирект-лимит — постоянное свойство сайта (не temp_error, T-048)")
		}
		if want := httpfetch.DefaultMaxRedirects + 1; len(chk.Redirects) != want {
			t.Errorf("цепочка в результате = %d шагов, want %d", len(chk.Redirects), want)
		}
		if r := httpDomainState(t, env.pool, byLabel["infinite-redirect"]); r.status != model.StatusNoHTTP {
			t.Errorf("статус = %s, want no_http", r.status)
		}
	})

	t.Run("SPA-оболочка: вердикт рендера и контент", func(t *testing.T) {
		res := run("spa-shell")
		recs := res.Subdomains[0].Records
		if len(recs) != 1 {
			t.Fatalf("записей: %d, want 1", len(recs))
		}
		rec := recs[0]
		if rec.SPA == nil || !rec.SPA.SPA {
			t.Fatal("SPA-вердикт не выставлен (T-057: публикация q:render — рендер-этап)")
		}
		reasons := map[httpfetch.SPAReason]bool{}
		for _, r := range rec.SPA.Reasons {
			reasons[r] = true
		}
		for _, want := range []httpfetch.SPAReason{
			httpfetch.SPAReasonLowText, httpfetch.SPAReasonRootContainer, httpfetch.SPAReasonNoScriptJS,
		} {
			if !reasons[want] {
				t.Errorf("признак %s не сработал (фикстура #root + noscript)", want)
			}
		}
		if rec.Charset != "utf-8" {
			t.Errorf("кодировка = %q, want utf-8", rec.Charset)
		}
		if !rec.StoreContent || rec.ContentHash == nil {
			t.Error("контент SPA-оболочки обязан сохраняться (рендер заменит его позже, T-067)")
		}
		rows := httpRows(t, env.pool, byLabel["spa-shell"])
		if len(rows) != 1 || rows[0].contentHash == nil {
			t.Fatalf("строки БД = %+v, want одна с контентом", rows)
		}
	})

	tlsCases := []struct {
		label    string
		problems []httpfetch.TLSProblem
	}{
		{"tls-self-signed", []httpfetch.TLSProblem{httpfetch.TLSProblemSelfSigned}},
		{"tls-expired", []httpfetch.TLSProblem{httpfetch.TLSProblemExpired, httpfetch.TLSProblemSelfSigned}},
		{"tls-wrong-san", []httpfetch.TLSProblem{httpfetch.TLSProblemHostnameMismatch, httpfetch.TLSProblemSelfSigned}},
	}
	for _, tc := range tlsCases {
		t.Run(tc.label+": загрузка + TLSInfo в записи", func(t *testing.T) {
			res := run(tc.label)
			chk := res.Subdomains[0].HTTPS
			if !chk.Available || chk.TLS == nil {
				t.Fatalf("https недоступен или TLS не разобран (некорректный сертификат не мешает, §8)")
			}
			if chk.TLS.Valid || len(chk.TLS.Problems) != len(tc.problems) {
				t.Fatalf("проверка сертификата = valid:%v problems:%v, want false %v",
					chk.TLS.Valid, chk.TLS.Problems, tc.problems)
			}
			rows := httpRows(t, env.pool, byLabel[tc.label])
			if len(rows) != 1 {
				t.Fatalf("строк: %d, want 1 (протоколы ведут на один сайт)", len(rows))
			}
			var tlsJSON string
			if err := env.pool.QueryRow(ctx,
				"SELECT tls::text FROM subdomain_results WHERE domain_id = $1", byLabel[tc.label],
			).Scan(&tlsJSON); err != nil {
				t.Fatalf("чтение tls jsonb: %v", err)
			}
			for _, p := range tc.problems {
				if !strings.Contains(tlsJSON, `"`+p.String()+`"`) {
					t.Errorf("tls jsonb без причины %s: %s", p, tlsJSON)
				}
			}
		})
	}
}

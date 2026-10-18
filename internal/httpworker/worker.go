// Package httpworker — HTTP-этап проверки доменов (T-053; ТЗ 5.5 пп. 1, 6,
// §4.2, §7). Задача q:http — домен целиком (формат T-024): воркер проверяет
// каждый DNS-живой субдомен по обоим протоколам (https и http; редиректы,
// лимиты, TLS и кодировки — цепочка httpfetch.FetchChain, T-048), параллельно
// внутри задачи (пул горутин Options.PerDomain), с общим кэшем «итоговый URL
// → результат» (ТЗ 5.5 п. 6: не скачивать одну и ту же страницу дважды —
// alias bare→www после редиректа на тот же URL второй загрузкой не
// скачивается). Результат домена собирается целиком: записи после
// вариант-логики (DecideVariants, T-054), алиасов (DetectAliases, T-055) и
// SPA-эвристики (DetectSPA, T-057) — и возвращается вызывающему.
//
// Запись результата в БД (subdomain_results, статус домена) и публикация
// q:analyze — Recorder (record.go, T-058), сборка сервиса ставит его в
// Options.OnResult вслед за ContentSaver (T-056); хук вызывается в цикле
// Run до подтверждения XACK — порядок «запись в БД → публикация → XACK»
// (ТЗ 4.2, §5.5 п. 9) собирается без правки цикла. Публикация q:render по
// SPA-записям и флаг render_pending при переполнении очереди рендера —
// renderq.go (T-066), вызывается из Recorder.
//
// Тела страниц (UTF-8, T-051) удерживаются в памяти до конца задачи
// домена: в фазе проверок — по одному экземпляру на скачанную страницу
// (кэш итоговых URL), после разметки алиасов — только у записей-
// оригиналов (Record.Body, StoreContent от T-055); сохранение контента
// в ContentStore — ContentSaver (store.go, T-056), после него байты
// отбрасываются. Потолок тела — max_body_bytes (2 МиБ, ТЗ 5.5), страниц
// в задаче не больше 2 × субдомены ≤ 126 (§5.10) — память задачи
// ограничена; DefaultConcurrency учитывает её при выборе окна задач.
package httpworker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/miekg/dns"

	"crawler/internal/content"
	"crawler/internal/dnsclient"
	"crawler/internal/httpfetch"
	"crawler/internal/metrics"
	"crawler/internal/model"
	"crawler/internal/queue"
)

// FetchFunc — шов загрузки (стиль dnsclient.exchangeFunc): одна проверка
// (субдомен × протокол) — один проход цепочки редиректов T-048.
// Продакшн-реализация — ChainFetch; юнит-тесты подменяют сценарием без
// сети.
type FetchFunc func(ctx context.Context, req httpfetch.FetchRequest, opts httpfetch.ChainOptions) (*httpfetch.ChainResult, error)

// ChainFetch строит FetchFunc поверх транспорта этапа (*httpfetch.Fetcher):
// обычный вызов httpfetch.FetchChainOpts. Отдельная обёртка, а не прямое
// использование FetchChainOpts воркером — шов подмены в тестах.
func ChainFetch(f *httpfetch.Fetcher) FetchFunc {
	return func(ctx context.Context, req httpfetch.FetchRequest, opts httpfetch.ChainOptions) (*httpfetch.ChainResult, error) {
		return httpfetch.FetchChainOpts(ctx, f, req, opts)
	}
}

// --- классификация ошибок проверки (ТЗ §8 «фиксируется код ошибки») --------

// ErrorClass — класс ошибки проверки субдомена по протоколу. Коды стабильны:
// T-058 кладёт класс в subdomain_results.error. Класс не роняет домен —
// ошибка остаётся в результате субдомена, финальный статус решает агрегатор
// эпизода (§7, T-058).
type ErrorClass int16

const (
	// ErrNone — проверка без ошибки (обрезанное тело отмечается отдельно
	// флагом Truncated, но тоже кодируется ErrBody — см. ниже).
	ErrNone ErrorClass = 0
	// ErrTimeout — таймаут одного из слоёв (connect/TLS/заголовки/общий,
	// анти-тарпит): httpfetch.IsTimeout. Временный класс (ТЗ 6.2:
	// temp_error — «SERVFAIL, таймауты»), повтор проверки имеет смысл.
	ErrTimeout ErrorClass = 1
	// ErrRedirects — цепочка прервана лимитом max_redirects или детектом
	// цикла (httpfetch.ErrTooManyRedirects): постоянное свойство сайта,
	// не temp_error (T-048).
	ErrRedirects ErrorClass = 2
	// ErrTLS — сбой TLS-рукопожатия по вине сертификата/протокола сервера
	// (httpfetch.IsTLSCertError): некорректный сертификат загрузке не
	// мешает (InsecureSkipVerify, ТЗ 5.5 п. 3), это именно отказ
	// установить соединение.
	ErrTLS ErrorClass = 3
	// ErrBody — тело ответа: неподдерживаемое/битое Content-Encoding,
	// zip-бомба (httpfetch.ErrDecompressTooLarge) или обрезка потолком
	// max_body_bytes (данные частичны, статус получен — Available при
	// этом true, ТЗ 8 «огромные ответы прерываются по лимитам»).
	ErrBody ErrorClass = 4
	// ErrNetwork — прочие транспортные ошибки: отказ соединения, обрыв,
	// ошибка резолва хоста редиректа. Сюда же попадает отмена ctx в
	// полёте (результат такой проверки всё равно отбрасывается — см.
	// ProcessDomain).
	ErrNetwork ErrorClass = 5
)

// String возвращает имя класса для логов.
func (c ErrorClass) String() string {
	switch c {
	case ErrNone:
		return "none"
	case ErrTimeout:
		return "timeout"
	case ErrRedirects:
		return "redirects"
	case ErrTLS:
		return "tls"
	case ErrBody:
		return "body"
	case ErrNetwork:
		return "network"
	default:
		return fmt.Sprintf("ErrorClass(%d)", int16(c))
	}
}

// IsTemp сообщает, что класс ошибки временный и повтор проверки может
// увенчаться успехом (ТЗ 6.2: temp_error — таймауты). Прочие классы —
// свойства сайта или его окружения, повтор в ближнем горизонте смысла не
// имеет (регулярная перепроверка — T-030).
func (c ErrorClass) IsTemp() bool { return c == ErrTimeout }

// workerClassifyError сводит ошибку загрузки к классу (порядок проверок
// важен: TooManyRedirects не является net.Error, таймаут анти-тарпита —
// является).
func workerClassifyError(err error) ErrorClass {
	switch {
	case err == nil:
		return ErrNone
	case errors.Is(err, httpfetch.ErrTooManyRedirects):
		return ErrRedirects
	case errors.Is(err, httpfetch.ErrDecompressTooLarge):
		return ErrBody
	case httpfetch.IsTimeout(err):
		return ErrTimeout
	case httpfetch.IsTLSCertError(err):
		return ErrTLS
	default:
		return ErrNetwork
	}
}

// --- кэш «итоговый URL → результат» (ТЗ 5.5 п. 6) --------------------------

// workerCacheKey — ключ кэша итоговых URL: схема + хост (без порта, нижний
// регистр — punycode гарантирует цепочка T-048) + путь (пустой — корень
// «/»). Согласован с критерием вариантов T-054 (NewVariantInput): query и
// фрагмент НЕ входят — счётчиковые параметры (?i=1, ?i=2, …) не разваливают
// дедупликацию одной страницы, а реальное различие контента при одном пути
// ловит хэш. Схема входит: http и https одного адреса — разные скачивания,
// подмена результата http-проверки https-страницей фабриковала бы
// доступность протокола, который реально не проверялся. Порт не входит,
// как в критерии вариантов: стандартный и нестандартный порт одного хоста
// — один сайт.
func workerCacheKey(u *url.URL) string {
	if u == nil {
		return ""
	}
	path := u.Path
	if path == "" {
		path = "/"
	}
	return strings.ToLower(u.Scheme) + "://" + strings.ToLower(u.Hostname()) + path
}

// workerPage — скачанная страница (итог цепочки) в кэше: хэш и SPA-вердикт
// вычислены при чтении; тело (UTF-8) удерживается до конца задачи — его
// заберёт запись-оригинал для сохранения контента (Record.Body, T-056),
// кэш-попадания делят те же байты (одна копия на скачанную страницу).
type workerPage struct {
	final     *url.URL
	status    int
	header    http.Header
	cookies   []*http.Cookie
	tls       *httpfetch.TLSInfo
	hash      *content.Hash
	body      []byte
	charset   string
	truncated bool
	spa       *httpfetch.SPAVerdict
}

// workerURLCache — общий кэш задачи домена (ТЗ 5.5 п. 6 «внутри воркера
// для задачи домена»): итоговые URL всех завершённых проверок. Живёт одну
// задачу: алиасы — свойство пары субдоменов одного домена, между доменами
// итоговые URL не пересекаются (внешние парковки не считаются — повторное
// использование чужого тела подменяло бы TLS/заголовки чужого соединения).
// Потокобезопасен: проверки субдоменов идут параллельно.
type workerURLCache struct {
	mu    sync.Mutex
	pages map[string]*workerPage
}

func workerNewURLCache() *workerURLCache {
	return &workerURLCache{pages: make(map[string]*workerPage, 8)}
}

// get возвращает страницу по ключу или nil.
func (c *workerURLCache) get(key string) *workerPage {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pages[key]
}

// put запоминает страницу по ключу итогового URL.
func (c *workerURLCache) put(key string, p *workerPage) {
	if key == "" || p == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pages[key] = p
}

// --- результат домена -------------------------------------------------------

// SchemeCheck — итог проверки субдомена по одному протоколу (ТЗ 5.5 пп. 1,
// 7). Ошибка не роняет домен: доступность — Available, условие результата —
// ErrClass (§8); тело в памяти не удерживается — от него остаются хэш,
// кодировка и SPA-вердикт.
type SchemeCheck struct {
	// Scheme — протокол проверки (стартового запроса), не итогового URL.
	Scheme model.Scheme
	// Available — проверка дошла до итогового ответа: сервер ответил любым
	// статусом, включая 4xx/5xx («протокол работает», критерий T-054).
	Available bool
	// Cached — итог взят из кэша итоговых URL без сети (ТЗ 5.5 п. 6):
	// стартовый URL этой проверки совпал с итоговым URL другой проверки
	// задачи (классика bare→www). ResponseMs такого итога — 0.
	Cached bool
	// FinalURL — итоговый URL (страница в конце цепочки, ТЗ 5.5 п. 4);
	// nil при ошибке загрузки.
	FinalURL *url.URL
	// Status — код статуса итогового ответа.
	Status int
	// Header — заголовки итогового ответа (выжимка обмена: только
	// итоговая страница, промежуточные ответы цепочки представлены
	// Redirects). T-058 сериализует в subdomain_results.headers.
	Header http.Header
	// Cookies — Set-Cookie итогового ответа (ТЗ 5.5 п. 7 «cookies»).
	Cookies []*http.Cookie
	// TLS — разбор сертификата итогового соединения (T-050); nil для http
	// и кэш-попаданий http-проверок (не ошибка, §8).
	TLS *httpfetch.TLSInfo
	// ResponseMS — время до заголовков итогового ответа, мс (Elapsed
	// последнего Fetch цепочки, ТЗ 5.5 п. 7 «время ответа»).
	ResponseMS int
	// Redirects — цепочка редиректов (T-048): URL и статус каждого шага,
	// последний элемент — итоговая страница. При ошибке — выполненная
	// часть; при кэш-попадании — синтетический единственный шаг.
	Redirects []httpfetch.RedirectStep
	// ContentHash — хэш UTF-8-нормализованного тела (content.HashOf,
	// ТЗ 5.7): дедупликация парковок и критерий вариантов/алиасов
	// (T-054/T-055). nil — тело не получено (ошибка); хэш пустого тела —
	// валидное значение. Само хранение контента — T-056.
	ContentHash *content.Hash
	// Truncated — тело обрезано потолком max_body_bytes (T-049): данные
	// частичны, не ошибка сети; хэш вычислен над обрезанными байтами.
	Truncated bool
	// Charset — каноническое имя кодировки ответа (ToUTF8, T-051).
	Charset string
	// Body — тело итоговой страницы в UTF-8 (T-051): те же байты, от
	// которых вычислен ContentHash (content.HashOf). Удерживается до
	// конца задачи домена — запись-оригинал (StoreContent, T-055)
	// переносит его в Record.Body для сохранения в хранилище (T-056);
	// после разметки алиасов проверки без оригинальных записей тело
	// отпускают. nil — тела нет (ошибка загрузки, обрыв чтения).
	Body []byte
	// SPA — вердикт эвристики по телу (DetectSPA, T-057); nil при ошибке
	// загрузки. Публикацию q:render по нему делает рендер-конвейер
	// T-066 (renderq.go) в цепочке записи результата — при переполнении
	// очереди домен получает флаг render_pending и дорабатывается позже.
	SPA *httpfetch.SPAVerdict
	// ErrClass — класс ошибки (ErrNone — без ошибки; обрезка тела — ErrBody
	// при Available=true). ErrText — исходный текст для логов и разбора.
	ErrClass ErrorClass
	ErrText  string
}

// Record — сохраняемая запись субдомен-результата после вариант-логики и
// алиасов: колонки subdomain_results (ТЗ 5.5 п. 7, §6.1). Данные записи —
// из проверки-источника (Scheme; вариант 0 при свёрнутых протоколах несёт
// данные https-проверки, T-054); записывает в БД T-058.
type Record struct {
	// Variant — 0 основной, 1 http (T-054).
	Variant model.Variant
	// Scheme — протокол проверки-источника записи.
	Scheme model.Scheme
	// SubdomainID, Label — идентичность записи (справочно для потребителя).
	SubdomainID int16
	Label       string
	// FinalURL — итоговый URL записи (строкой, как в subdomain_results.
	// final_url); Host/Path — его нормализованное представление для
	// сравнений (критерии T-054/T-055).
	FinalURL string
	Host     string
	Path     string
	// HTTPStatus, Header, Cookies, TLS, ResponseMS, Redirects — данные
	// проверки-источника (см. SchemeCheck).
	HTTPStatus int
	Header     http.Header
	Cookies    []*http.Cookie
	TLS        *httpfetch.TLSInfo
	ResponseMS int
	Redirects  []httpfetch.RedirectStep
	// ContentHash, Truncated, Charset, SPA — контент проверки-источника.
	ContentHash *content.Hash
	Truncated   bool
	Charset     string
	SPA         *httpfetch.SPAVerdict
	// Body — тело проверки-источника в UTF-8 (те же байты, от которых
	// вычислен ContentHash): заполняется у записей-оригиналов с хэшем
	// без обрезки — ContentSaver (T-056) пишет его в ContentStore.Put
	// и отбрасывает. nil — тело записи не положено (алиас, обрезанное
	// тело, контента нет) либо сохранение уже выполнено.
	Body []byte
	// ContentNotStored — тело получено (хэш вычислен), но контент НЕ
	// сохранён: переполнение диска (ErrStoreFull, ТЗ 8), лимит
	// max_store_bytes (ТЗ 5.5) или обрезка тела потолком max_body_bytes.
	// Запись живёт с content_hash = NULL: content_hash — ссылка на
	// contents (§6.1), без блока в хранилище строки нет; повторная
	// проверка домена (recheck, T-030) скачает страницу и сохранит.
	// У алиасов не ставится: их контент — в записи-оригинале (T-055).
	ContentNotStored bool
	// AliasKind, AliasSubdomainID, StoreContent — решение DetectAliases
	// (T-055): вид связи с субдоменом-оригиналом и признак «контент
	// хранится один раз у оригинала» (само сохранение — T-056).
	AliasKind        model.AliasKind
	AliasSubdomainID *int16
	StoreContent     bool
	// ErrClass — класс ошибки проверки-источника (ErrBody у обрезанного
	// тела при живом статусе; для доступных записей обычно ErrNone).
	ErrClass ErrorClass
}

// SubdomainResult — итог субдомена задачи: обе проверки протоколов и
// записи, которые создала вариант-логика (T-054).
type SubdomainResult struct {
	SubdomainID int16
	Label       string
	// Hostname — полное имя субдомена (label + "." + домен), punycode.
	Hostname string
	// Addrs, CNAME — данные DNS-этапа из задачи (ТЗ 5.4 п. 4), переносятся
	// в результат (ТЗ 5.5 п. 7 «сохраняется всё: адреса, CNAME»).
	Addrs []netip.Addr
	CNAME string
	// HTTPS, HTTP — проверки по протоколам (порядок полей — порядок
	// запуска в пуле).
	HTTPS SchemeCheck
	HTTP  SchemeCheck
	// Records — записи после вариант-логики и алиасов; пусто — протоколы
	// не ответили (вклад в «нет ответа ни у кого», §7).
	Records []Record
}

// DomainResult — полный итог HTTP-эпизода домена (ТЗ 5.5, §7): обобщённая
// структура для записи T-058 — статус домена (alive/no_http/temp_error по
// §6.2/§7), строки subdomain_results, постановка q:render.
type DomainResult struct {
	ZoneID   int16
	DomainID int64
	Name     string
	// Subdomains — порядок задачи q:http.
	Subdomains []SubdomainResult
}

// RecordCount — число записей результата (субдомен × вариант).
func (r *DomainResult) RecordCount() int {
	n := 0
	for i := range r.Subdomains {
		n += len(r.Subdomains[i].Records)
	}
	return n
}

// HasRecords — ответил ли хоть один субдомен по хоть одному протоколу
// (§7: «нет ответа ни у кого → status=no_http»; статус присвоит T-058).
func (r *DomainResult) HasRecords() bool { return r.RecordCount() > 0 }

// AnyTimeout — есть ли среди проверок таймаут-класс (ErrTimeout): данные
// для решения T-058 между no_http (веб молчит постоянно) и temp_error
// («SERVFAIL, таймауты», ТЗ 6.2) — у целиком «замолчавшего» домена с
// таймаутами повтор эпизода осмыслен.
func (r *DomainResult) AnyTimeout() bool {
	for i := range r.Subdomains {
		if r.Subdomains[i].HTTPS.ErrClass == ErrTimeout || r.Subdomains[i].HTTP.ErrClass == ErrTimeout {
			return true
		}
	}
	return false
}

// --- воркер ------------------------------------------------------------------

// Дефолты воркера.
const (
	// DefaultGroup — группа потребителей q:http (ТЗ 4.2: consumer groups
	// масштабируют воркеров горизонтально).
	DefaultGroup = "http-worker"

	// DefaultConcurrency — одновременных задач q:http (доменов) на
	// экземпляр в цикле Run. Скромное число против http.concurrency=2000:
	// глобальный потолок соединений и RPS держит транспорт (T-049,
	// Options.Limits — семафор GlobalConns), задачи лишь заполняют его;
	// каждая задача держит в памяти результат домена — заголовки, хэши
	// и до сохранения контента (T-056) тела записей-оригиналов (≤ 2 МиБ
	// каждое, оригиналов ≤ записей ≤ 126) — типичная задача единицы МБ.
	// Масштабирование нагрузки — инстансами (ТЗ 4.1 «масштабируется
	// количеством горутин и процессов»), потолок поднимается
	// Options.Concurrency.
	DefaultConcurrency = 64

	// DefaultConsume — записей q:http за одно чтение: меньше
	// DefaultConcurrency (окно чтения не глубже окна обработки).
	DefaultConsume = 16

	// DefaultPerDomain — параллельных проверок внутри задачи домена
	// (субдомен × протокол). Субдоменов ≤ 63 (§5.10), проверок вдвое
	// больше — но умеренный пул: субдомены одного домена почти всегда
	// живут на одних и тех же IP (один сервер, CNAME-хостинги), и
	// реальный параллелизм подключения к одному IP уже ограничен
	// семафором per-IP транспорта (3–4, ТЗ 5.5); 8 = удвоенный per-IP
	// потолок покрывает домены с несколькими адресами (апекс/www
	// отдельно, CNAME-цели) и две проверки одного субдомена, не выделяя
	// одному домену несправедливую долю глобального бюджета.
	DefaultPerDomain = 8

	// DefaultErrorWait — пауза после ошибок Redis (стиль T-039):
	// холостые круги не греют логи, неподтверждённые доставки вернёт
	// XAUTOCLAIM (T-025).
	DefaultErrorWait = 30 * time.Second
)

// Options — параметры воркера; нулевые значения заменяются дефолтами
// (withDefaults, стиль dnsworker.Options).
type Options struct {
	// Concurrency — одновременных задач q:http в цикле Run; 0 —
	// DefaultConcurrency.
	Concurrency int
	// Consume — записей q:http за одно чтение; 0 — DefaultConsume.
	Consume int
	// Group — группа потребителей q:http; "" — DefaultGroup.
	Group string
	// Consumer — имя потребителя группы; "" — "http-worker-<pid>".
	Consumer string
	// PerDomain — параллельных проверок (субдомен × протокол) внутри
	// задачи домена; 0 — DefaultPerDomain.
	PerDomain int
	// MaxRedirects — лимит цепочки редиректов (http.max_redirects,
	// T-048): 0 — не следовать вовсе; отрицательные значения — дефолт
	// таблицы лимитов ТЗ 5.5 (httpfetch.DefaultMaxRedirects).
	MaxRedirects int
	// Log — логгер; nil — записи подавлены (slog.DiscardHandler).
	Log *slog.Logger
	// Metrics — реестр Prometheus сервиса (T-077, ТЗ 5.9): из него New
	// строит приборы этапа (NewMetrics); nil — без метрик (изолированные
	// сборки и тесты).
	Metrics *metrics.Registry
	// OnResult — обработчик готового результата домена, вызывается в
	// цикле Run ПОСЛЕ обработки и ДО XACK. Сборка сервиса (cmd/http-worker)
	// ставит сюда цепочку: ContentSaver (T-056) → Recorder — запись
	// subdomain_results, статус домена и публикация q:analyze (T-058).
	// nil — результат собирается и отбрасывается (изолированные тесты).
	// Ошибка обработчика — доставка без XACK, вернётся повторной
	// доставкой (at-least-once, ТЗ 3.4).
	OnResult func(ctx context.Context, res *DomainResult) error
}

func (o Options) withDefaults() Options {
	if o.Concurrency <= 0 {
		o.Concurrency = DefaultConcurrency
	}
	if o.Consume <= 0 {
		o.Consume = DefaultConsume
	}
	if o.Group == "" {
		o.Group = DefaultGroup
	}
	if o.Consumer == "" {
		o.Consumer = fmt.Sprintf("http-worker-%d", os.Getpid())
	}
	if o.PerDomain <= 0 {
		o.PerDomain = DefaultPerDomain
	}
	if o.MaxRedirects < 0 {
		o.MaxRedirects = httpfetch.DefaultMaxRedirects
	}
	return o
}

func (o Options) log() *slog.Logger {
	if o.Log == nil {
		return slog.New(slog.DiscardHandler)
	}
	return o.Log
}

// Worker — HTTP-воркер: обрабатывает задачи q:http (домены) через шов
// загрузки FetchFunc. Потокобезопасен: состояние неизменно после New.
type Worker struct {
	fetch FetchFunc
	opt   Options
	met   *Metrics // приборы этапа (T-077); nil — no-op
}

// New создаёт воркер поверх шва загрузки fetch (продакшн — ChainFetch).
func New(fetch FetchFunc, opts Options) (*Worker, error) {
	if fetch == nil {
		return nil, errors.New("httpworker: шов загрузки не задан (ChainFetch)")
	}
	return &Worker{fetch: fetch, opt: opts.withDefaults(), met: NewMetrics(opts.Metrics)}, nil
}

// workerSub — разобранный субдомен задачи: адреса распарсены один раз.
type workerSub struct {
	id    int16
	label string
	host  string
	addrs []netip.Addr
	cname string
}

// workerPrepareTask валидирует задачу (queue.Decode уже проверил её в
// цикле — здесь защита прямых вызовов ProcessDomain) и парсит адреса.
func workerPrepareTask(task queue.HTTPTask) ([]workerSub, error) {
	if strings.TrimSpace(task.Name) == "" {
		return nil, errors.New("httpworker: задача без имени домена")
	}
	if len(task.Subdomains) == 0 {
		return nil, errors.New("httpworker: задача без субдоменов (пустая q:http не публикуется, T-024)")
	}
	seen := make(map[int16]struct{}, len(task.Subdomains))
	subs := make([]workerSub, 0, len(task.Subdomains))
	for i, s := range task.Subdomains {
		if err := model.ValidateSubdomainID(int(s.SubdomainID)); err != nil {
			return nil, fmt.Errorf("httpworker: субдомен №%d: %w", i+1, err)
		}
		if _, dup := seen[s.SubdomainID]; dup {
			return nil, fmt.Errorf("httpworker: субдомен №%d: дубликат subdomain_id %d", i+1, s.SubdomainID)
		}
		seen[s.SubdomainID] = struct{}{}
		if strings.ContainsAny(s.Label, ". ") {
			return nil, fmt.Errorf("httpworker: субдомен №%d: метка %q — одна DNS-метка", i+1, s.Label)
		}
		host := task.Name
		if s.Label != "" {
			host = s.Label + "." + task.Name
		}
		addrs := make([]netip.Addr, 0, len(s.Addrs))
		for _, a := range s.Addrs {
			addr, err := netip.ParseAddr(a)
			if err != nil {
				return nil, fmt.Errorf("httpworker: субдомен №%d: адрес %q: %w", i+1, a, err)
			}
			addrs = append(addrs, addr)
		}
		if len(addrs) == 0 {
			return nil, fmt.Errorf("httpworker: субдомен №%d: нет адресов", i+1)
		}
		subs = append(subs, workerSub{id: s.SubdomainID, label: s.Label, host: host, addrs: addrs, cname: s.CNAME})
	}
	return subs, nil
}

// ProcessDomain обрабатывает задачу q:http целиком (ТЗ 5.5 пп. 1, 6, §7):
// проверки субдоменов по обоим протоколам параллельно (пул PerDomain) с
// общим кэшем итоговых URL → вариант-логика (T-054) → алиасы (T-055).
// Ошибки отдельных проверок остаются в их результатах и не роняют домен
// (§8); ошибка возврата — только негодная задача или отмена ctx (эпизод не
// завершён, доставка q:http не подтверждается и повторится, ТЗ 3.4).
func (w *Worker) ProcessDomain(ctx context.Context, task queue.HTTPTask) (*DomainResult, error) {
	subs, err := workerPrepareTask(task)
	if err != nil {
		return nil, err
	}

	res := &DomainResult{
		ZoneID:     task.ZoneID,
		DomainID:   task.DomainID,
		Name:       task.Name,
		Subdomains: make([]SubdomainResult, len(subs)),
	}
	cache := workerNewURLCache()

	// Пул горутин: семафор PerDomain приобретается до запуска (очередь
	// горутин не растёт, стиль dnsworker.processBatch); записи в разные
	// поля SubdomainResult — разные ячейки памяти, гонки нет.
	sem := make(chan struct{}, w.opt.PerDomain)
	var wg sync.WaitGroup
	for i, sub := range subs {
		res.Subdomains[i] = SubdomainResult{
			SubdomainID: sub.id,
			Label:       sub.label,
			Hostname:    sub.host,
			Addrs:       sub.addrs,
			CNAME:       sub.cname,
		}
		// https и http одного субдомена — независимые проверки; Scheme
		// каждой проверки фиксируется в её результате самим checkScheme.
		wg.Add(2)
		sem <- struct{}{}
		go func(dst *SchemeCheck, sub workerSub) {
			defer wg.Done()
			defer func() { <-sem }()
			*dst = *w.checkScheme(ctx, sub.host, sub.addrs, model.SchemeHTTPS, cache)
		}(&res.Subdomains[i].HTTPS, sub)
		sem <- struct{}{}
		go func(dst *SchemeCheck, sub workerSub) {
			defer wg.Done()
			defer func() { <-sem }()
			*dst = *w.checkScheme(ctx, sub.host, sub.addrs, model.SchemeHTTP, cache)
		}(&res.Subdomains[i].HTTP, sub)
	}
	wg.Wait()

	// Отмена/дедлайн — эпизод не завершён: результат с провалившимися
	// проверками выглядел бы «веб молчит» и дал бы ложный no_http;
	// честный сбой вернёт задачу повторной доставкой (ТЗ 3.4).
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("httpworker: домен %s: отмена: %w", task.Name, err)
	}

	// Вариант-логика (T-054) по каждому субдомену; алиасы (T-055) — по
	// всем записям домена сразу (классы эквивалентности склеивают
	// субдомены, §5.5 п. 6). Указатели записей берутся из уже собранного
	// результата (append в локальный срез реаллокацией отрывал бы их от
	// SubdomainResult.Records).
	var (
		aliasIn  []httpfetch.AliasInput
		aliasRec []*Record
	)
	for i := range res.Subdomains {
		sub := &res.Subdomains[i]
		httpsIn := httpfetch.NewVariantInput(model.SchemeHTTPS, sub.HTTPS.Available, sub.HTTPS.FinalURL, sub.HTTPS.ContentHash)
		httpIn := httpfetch.NewVariantInput(model.SchemeHTTP, sub.HTTP.Available, sub.HTTP.FinalURL, sub.HTTP.ContentHash)
		variants, err := httpfetch.DecideVariants(httpsIn, httpIn)
		if err != nil {
			return nil, fmt.Errorf("httpworker: домен %s: субдомен %s: %w", task.Name, sub.Hostname, err)
		}
		recs := make([]Record, 0, len(variants))
		for _, vr := range variants {
			src := &sub.HTTPS
			if vr.Scheme == model.SchemeHTTP {
				src = &sub.HTTP
			}
			recs = append(recs, Record{
				Variant:     vr.Variant,
				Scheme:      vr.Scheme,
				SubdomainID: sub.SubdomainID,
				Label:       sub.Label,
				FinalURL:    src.FinalURL.String(),
				Host:        src.FinalURL.Hostname(),
				Path:        src.FinalURL.Path,
				HTTPStatus:  src.Status,
				Header:      src.Header,
				Cookies:     src.Cookies,
				TLS:         src.TLS,
				ResponseMS:  src.ResponseMS,
				Redirects:   src.Redirects,
				ContentHash: src.ContentHash,
				Truncated:   src.Truncated,
				Charset:     src.Charset,
				SPA:         src.SPA,
				ErrClass:    src.ErrClass,
				AliasKind:   model.AliasNone,
			})
		}
		sub.Records = recs
		for j := range sub.Records {
			r := &sub.Records[j]
			aliasIn = append(aliasIn, httpfetch.AliasInput{
				SubdomainID: sub.SubdomainID,
				Label:       sub.Label,
				FinalHost:   r.Host,
				FinalPath:   r.Path,
				Scheme:      r.Scheme,
				Variant:     r.Variant,
				ContentHash: workerHashBytes(r.ContentHash),
			})
			aliasRec = append(aliasRec, r)
		}
	}

	// DetectAliases возвращает решение параллельно входу.
	for i, ar := range httpfetch.DetectAliases(task.Name, aliasIn) {
		rec := aliasRec[i]
		rec.AliasKind = ar.AliasKind
		rec.AliasSubdomainID = ar.AliasSubdomainID
		rec.StoreContent = ar.StoreContent
	}

	// Тела оригиналов (T-056): запись-оригинал класса несёт тело для
	// сохранения в ContentStore (ContentSaver.Save); алиасы, записи без
	// контента и обрезанные тела (хэш усечённых байт врал бы при дедупе,
	// частичность уже отмечена Truncated, T-049) тела не получают. После
	// разметки тела проверок отпускаются — Record.Body остаётся
	// единственным владельцем, в памяти только тела оригиналов.
	for i := range res.Subdomains {
		sub := &res.Subdomains[i]
		for j := range sub.Records {
			r := &sub.Records[j]
			if r.StoreContent && r.ContentHash != nil && !r.Truncated {
				src := &sub.HTTPS
				if r.Scheme == model.SchemeHTTP {
					src = &sub.HTTP
				}
				r.Body = src.Body
			}
		}
		sub.HTTPS.Body = nil
		sub.HTTP.Body = nil
	}
	return res, nil
}

// workerHashBytes — хэш как срез для AliasInput.ContentHash (nil — без
// контента, контракт T-055); копия — запись в результате остаётся владыкой.
func workerHashBytes(h *content.Hash) []byte {
	if h == nil {
		return nil
	}
	b := make([]byte, len(h))
	copy(b, h[:])
	return b
}

// workerBodyHardLimit — страховочный потолок чтения тела в самом воркере
// на случай транспорта без лимитов T-049 (изолированные сборки и юнит-
// тесты без Options.Limits): неограниченное чтение раздувало бы память
// на злонамеренном ответе (§9 — недоверенный контент). С включёнными
// лимитами (боевой сборщик NewOptions включает их всегда) тело уже
// обёрнуто BodyLimiter с потолком http.max_body_bytes — сколь угодно
// выше дефолта, — и чтение идёт под ним без собственного потолка
// воркера: страховка не должна подменять настроенный лимит.
const workerBodyHardLimit = httpfetch.DefaultLimitBodyBytes

// checkScheme — одна проверка (субдомен × протокол, ТЗ 5.5 п. 1): кэш
// итоговых URL → цепочка редиректов (шов fetch) → чтение тела с потолком →
// UTF-8 (T-051) → хэш и SPA-вердикт (T-057). Все ошибки — в результат
// проверки; отмена ctx видна вызывающему через ctx.Err() после пула.
func (w *Worker) checkScheme(
	ctx context.Context,
	host string,
	addrs []netip.Addr,
	scheme model.Scheme,
	cache *workerURLCache,
) *SchemeCheck {
	check := &SchemeCheck{Scheme: scheme}
	start := &url.URL{Scheme: scheme.String(), Host: host, Path: "/"}

	// Кэш итоговых URL (ТЗ 5.5 п. 6): стартовый URL этой проверки уже
	// скачан другой проверкой задачи (bare→www) — вторая загрузка не
	// нужна. Кэш-итог честно помечается Cached: сеть не трогалась.
	if page := cache.get(workerCacheKey(start)); page != nil {
		check.Available = true
		check.Cached = true
		check.FinalURL = start
		check.Status = page.status
		check.Header = page.header
		check.Cookies = page.cookies
		check.TLS = page.tls
		check.Redirects = []httpfetch.RedirectStep{{URL: start.String(), Status: page.status}}
		check.ContentHash = page.hash
		check.Truncated = page.truncated
		check.Charset = page.charset
		check.Body = page.body
		check.SPA = page.spa
		return check
	}

	chain, err := w.fetch(ctx, httpfetch.FetchRequest{URL: start, Addrs: addrs}, httpfetch.ChainOptions{MaxRedirects: w.opt.MaxRedirects})
	if err != nil {
		check.ErrClass = workerClassifyError(err)
		check.ErrText = err.Error()
		if chain != nil {
			check.Redirects = chain.Chain // выполненная часть (T-048)
		}
		return check
	}
	if chain == nil || chain.Result == nil {
		// Редирект-лимит/цикл: Result nil, Chain — выполненная часть.
		check.ErrClass = ErrRedirects
		check.ErrText = httpfetch.ErrTooManyRedirects.Error()
		if chain != nil {
			check.Redirects = chain.Chain
		}
		return check
	}

	res := chain.Result
	check.Available = true
	check.FinalURL = res.FinalURL
	check.Status = res.StatusCode
	check.Header = res.Header
	check.Redirects = chain.Chain
	check.ResponseMS = int(res.Elapsed / time.Millisecond)
	check.TLS = res.TLSInfo
	for _, line := range res.Header.Values("Set-Cookie") {
		if c, err := http.ParseSetCookie(line); err == nil && c != nil {
			check.Cookies = append(check.Cookies, c)
		}
	}

	// Тело: читается целиком, закрывается всегда (общий таймаут
	// запроса живёт до Close, T-047), приводится к UTF-8 (T-051);
	// дальше нужны только хэш (T-054/T-055/T-056) и SPA-вердикт
	// (T-057). Потолок — BodyLimiter T-049, когда лимиты включены:
	// чтение режется по http.max_body_bytes (признак — Truncated),
	// анти-тарпит там же; без лимитёра — страховочный
	// workerBodyHardLimit от недоверенного ответа.
	var (
		body      []byte
		readErr   error
		truncated bool
	)
	if res.BodyLimiter != nil {
		body, readErr = io.ReadAll(res.Body)
		truncated = res.BodyLimiter.Truncated()
	} else {
		body, readErr = io.ReadAll(io.LimitReader(res.Body, workerBodyHardLimit+1))
		truncated = int64(len(body)) > workerBodyHardLimit
		if truncated {
			body = body[:workerBodyHardLimit]
		}
	}
	closeErr := res.Body.Close()
	if readErr == nil {
		readErr = closeErr
	}
	if readErr != nil {
		// Чтение сорвалось после заголовков (тарпит-таймаут, обрыв):
		// протокол ответил, страницы нет — Available гасится, частичный
		// статус/цепочка остаются для разбора (§8).
		check.Available = false
		check.ErrClass = workerClassifyError(readErr)
		check.ErrText = readErr.Error()
		return check
	}
	if truncated {
		check.Truncated = true
		check.ErrClass = ErrBody
		check.ErrText = "тело обрезано потолком max_body_bytes (данные частичны)"
	}

	cs := httpfetch.ToUTF8(res.Header, body)
	check.Charset = cs.Charset
	hash := content.HashOf(cs.UTF8)
	check.ContentHash = &hash
	check.Body = cs.UTF8
	spa := httpfetch.DetectSPA(cs.UTF8)
	check.SPA = &spa

	// Итоговый URL — в кэш задачи: следующие проверки, начинающиеся на
	// нём (www после редиректа bare), страницы повторно не скачивают.
	cache.put(workerCacheKey(res.FinalURL), &workerPage{
		final:     res.FinalURL,
		status:    check.Status,
		header:    check.Header,
		cookies:   check.Cookies,
		tls:       check.TLS,
		hash:      check.ContentHash,
		body:      check.Body,
		charset:   check.Charset,
		truncated: check.Truncated,
		spa:       check.SPA,
	})
	return check
}

// --- рабочий цикл ------------------------------------------------------------

// Run — рабочий цикл http-worker (ТЗ 4.2, §5.5): читает задачи q:http
// группой потребителей окном Consume и обрабатывает доставки параллельно
// до Concurrency доменов, каждую — паттерном Processing (T-025): обработка
// (ProcessDomain + Options.OnResult) → XACK. Работает до отмены ctx и
// возвращает nil — контракт OnRun каркаса (T-012: начатые задачи дорабатывают
// или возвращаются очереди повторной доставкой). Ошибка чтения или обработки
// не останавливает цикл: логируется (Warn), неподтверждённая доставка
// вернётся XAUTOCLAIM (ТЗ 4.2).
func Run(ctx context.Context, q queue.Queue, w *Worker, opts Options) error {
	if q == nil {
		return errors.New("httpworker: цикл воркера: очередь не задана")
	}
	if w == nil {
		return errors.New("httpworker: цикл воркера: воркер не задан")
	}
	o := opts.withDefaults()
	log := o.log()
	log.Info("цикл http-worker",
		"group", o.Group, "consumer", o.Consumer,
		"concurrency", o.Concurrency, "consume", o.Consume, "per_domain", o.PerDomain,
		"max_redirects", o.MaxRedirects, "on_result", o.OnResult != nil)

	wait := func(d time.Duration) {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-ctx.Done():
		case <-t.C:
		}
	}

	sem := make(chan struct{}, o.Concurrency)
	for {
		deliveries, err := q.Consume(ctx, queue.StreamHTTP, o.Group, o.Consumer, o.Consume)
		if err != nil {
			if ctx.Err() != nil {
				return nil // graceful shutdown
			}
			log.Warn("цикл http-worker: чтение q:http", "err", err)
			wait(DefaultErrorWait)
			continue
		}

		// Окно доставок обрабатывается параллельно до Concurrency;
		// доставка — один домен (ТЗ 4.2), параллелизм этапа — именно
		// здесь (у dns-worker домены параллелятся внутри пачки).
		var wg sync.WaitGroup
		for _, d := range deliveries {
			if ctx.Err() != nil {
				break // не начатые доставки окна не подтверждены
			}
			sem <- struct{}{}
			wg.Add(1)
			go func(d queue.Delivery) {
				defer wg.Done()
				defer func() { <-sem }()
				err := queue.Processing(ctx, q, o.Group, d, func(c context.Context, del queue.Delivery) error {
					return w.processDelivery(c, del, o)
				})
				if err != nil && ctx.Err() == nil {
					log.Warn("задача q:http не подтверждена — вернётся повторной доставкой",
						"entry", d.ID, "err", err)
				}
			}(d)
		}
		wg.Wait()
		if ctx.Err() != nil {
			return nil // остановка: окно доработано или возвращено очереди
		}
	}
}

// processDelivery обрабатывает одну доставку q:http. Подтверждение (XACK)
// выполняет Processing после nil-возврата — как требует ТЗ 4.2 («XACK
// только после записи результата в БД»): запись результата и публикация
// q:analyze идут в Options.OnResult (ContentSaver T-056 → Recorder T-058,
// сборка cmd/http-worker) до возврата, и порядок «запись → публикация →
// XACK» сохраняется без правки цикла. Успешно завершённая задача идёт
// в приборы этапа (T-077): пропускная способность, длительность,
// финальный статус, ошибки проверок, время ответа, записи rendered=false.
func (w *Worker) processDelivery(ctx context.Context, del queue.Delivery, o Options) error {
	task, err := queue.Decode[queue.HTTPTask](del)
	if err != nil {
		// Ядовитая запись: повторная доставка бессмысленна, очередь
		// исчерпает попытки и унесёт её в q:dead (T-025).
		return fmt.Errorf("httpworker: запись %s стрима %s: %w", del.ID, del.Stream, err)
	}
	start := time.Now()
	res, err := w.ProcessDomain(ctx, task)
	if err != nil {
		return fmt.Errorf("httpworker: домен %s: %w", task.Name, err)
	}
	if o.OnResult != nil {
		if err := o.OnResult(ctx, res); err != nil {
			return fmt.Errorf("httpworker: домен %s: обработка результата: %w", task.Name, err)
		}
	}
	w.met.Episode(res, time.Since(start))
	o.log().Debug("HTTP-задача обработана",
		"domain", task.Name, "subdomains", len(res.Subdomains), "records", res.RecordCount())
	return nil
}

// --- резолв хостов редиректов «через тот же резолвер» (ТЗ 5.5 п. 2) --------

// DNSLookup — адаптер DNS-этапа (dnsclient.Resolver с лимитами T-035) в
// httpfetch.Resolver: хосты HTTP-редиректов и meta refresh резолвятся тем
// же резолвером, что и DNS-этап («через тот же резолвер с кэшем», ТЗ 5.5
// п. 2), а не системным /etc/resolv.conf. Кэш — обёртка
// httpfetch.NewCachingResolver в сборке сервиса (cmd/http-worker).
type DNSLookup struct {
	resolve dnsclient.Resolver
	ipv6    bool
}

// NewDNSLookup строит адаптер; ipv6 — dns.ipv6 (ТЗ 5.4: AAAA отключаются
// конфигом без IPv6 — резолв редирект-хостов ищет только A).
func NewDNSLookup(resolve dnsclient.Resolver, ipv6 bool) *DNSLookup {
	return &DNSLookup{resolve: resolve, ipv6: ipv6}
}

// LookupNetIP резолвит хост для подключения (httpfetch.Resolver): A, при
// ipv6 — и AAAA; IP-литералы (Location вида http://192.0.2.1/) проходят
// без обмена. CNAME разворачивает сам рекурсор (unbound, ТЗ 5.4) — в
// ответе на A лежат и записи цели (RFC 1034 §4.3.2, T-037). Отрицание
// (NXDOMAIN) и пустой ответ — ошибка: подключаться не к чему.
func (d *DNSLookup) LookupNetIP(ctx context.Context, host string) ([]netip.Addr, error) {
	if ip, err := netip.ParseAddr(host); err == nil {
		return []netip.Addr{ip}, nil
	}
	types := []uint16{dns.TypeA}
	if d.ipv6 {
		types = append(types, dns.TypeAAAA)
	}
	var out []netip.Addr
	for _, t := range types {
		res, err := d.resolve.Exchange(ctx, dnsclient.Question{Name: host, Type: t})
		if err != nil {
			return nil, fmt.Errorf("httpworker: резолв %q %s: %w", host, dns.TypeToString[t], err)
		}
		if res.RCode != dnsclient.RCodeNOERROR {
			return nil, fmt.Errorf("httpworker: резолв %q %s: %s", host, dns.TypeToString[t], res.RCode.String())
		}
		for _, rr := range res.RecordsOf(t) {
			switch r := rr.(type) {
			case *dns.A:
				if addr, ok := netip.AddrFromSlice(r.A.To4()); ok {
					out = append(out, addr)
				}
			case *dns.AAAA:
				if addr, ok := netip.AddrFromSlice(r.AAAA.To16()); ok {
					out = append(out, addr.Unmap())
				}
			}
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("httpworker: резолв %q: пустой ответ", host)
	}
	return out, nil
}

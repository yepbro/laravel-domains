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
// Тела страниц (UTF-8, T-051) в памяти не удерживаются (T-147): тело
// читается с потолком, от него вычисляются хэш (критерий вариантов
// T-054 и алиасов T-055 — сравнение всегда по хэшам) и SPA-вердикт
// (T-057), затем немедленно сохраняется в ContentStore (Options.Store,
// Put по хэшу) и отбрасывается — слот задачи не держит тела дольше
// получения + Put (память слота: только метаданные и PerDomain
// буферов читаемых тел). Ссылки, добавленные ранними Put, сверяются с
// записями результата в ContentSaver (store.go): страница, не ставшая
// чьей-либо строкой subdomain_results (свёрнутый вариант, проигравший
// класс алиасов, повторное скачивание того же контента), отпускает
// ссылку Delete; ставшая — переносится в строку записи (T-056).
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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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

// workerCacheKey — ключ кэша итоговых URL: схема + хост (нижний
// регистр — punycode гарантирует цепочка T-048) + путь (пустой — корень
// «/»). Согласован с критерием вариантов T-054 (NewVariantInput): query и
// фрагмент НЕ входят — счётчиковые параметры (?i=1, ?i=2, …) не разваливают
// дедупликацию одной страницы, а реальное различие контента при одном пути
// ловит хэш. Схема входит: http и https одного адреса — разные скачивания,
// подмена результата http-проверки https-страницей фабриковала бы
// доступность протокола, который реально не проверялся. Явно указанный
// нестандартный порт входит по той же причине: страница host:8443 —
// ответ другого сервиса, кэш-попадание подменяло бы результат проверки
// порта по умолчанию контентом непроверенного порта; дефолтные порты
// схем (80/443) нормализуются away — явный :443 у https тот же адрес.
func workerCacheKey(u *url.URL) string {
	if u == nil {
		return ""
	}
	path := u.Path
	if path == "" {
		path = "/"
	}
	host := strings.ToLower(u.Hostname())
	if p := u.Port(); p != "" && p != workerSchemeDefaultPort(u.Scheme) {
		host += ":" + p
	}
	return strings.ToLower(u.Scheme) + "://" + host + path
}

// workerSchemeDefaultPort — дефолтный порт схемы (нормализация ключа:
// явный :443 у https — тот же адрес, что и без порта); прочие схемы в
// конвейере не встречаются — дефолта нет.
func workerSchemeDefaultPort(scheme string) string {
	if strings.ToLower(scheme) == "https" {
		return "443"
	}
	return "80"
}

// workerPage — скачанная страница (итог цепочки) в кэше: хэш и SPA-вердикт
// вычислены при чтении, тело сохранено ранним Put (T-147) и в памяти не
// живёт — кэш-попадания делят хэш и признаки страницы, байты лежат в
// хранилище (ContentStore.Get по хэшу). Класс/текст ошибки (ErrBody
// обрезанной страницы) переносится кэш-попаданию: один и тот же контент
// классифицируется одинаково независимо от того, скачала его проверка
// или взяла из кэша.
type workerPage struct {
	final      *url.URL
	status     int
	header     http.Header
	cookies    []*http.Cookie
	tls        *httpfetch.TLSInfo
	hash       *content.Hash
	stored     bool // ранний Put прошёл: ссылка в хранилище ждёт строку записи (T-147)
	charset    string
	truncated  bool
	responseMS int
	spa        *httpfetch.SPAVerdict
	errClass   ErrorClass
	errText    string
}

// workerPut — вход реестра ранних Put задачи (T-147): хэш сохранённой
// страницы и факт записи новых байтов (для доли дедупликации, ТЗ 11.4
// п. 3). Реестр собирается в кэше задачи (workerURLCache) по мере
// скачиваний и передаётся в DomainResult: ContentSaver.Save сверяет его
// с записями — ссылки, не ставшие строками, снимаются Delete.
type workerPut struct {
	hash    content.Hash
	created bool
}

// workerURLCache — общий кэш задачи домена (ТЗ 5.5 п. 6 «внутри воркера
// для задачи домена»): итоговые URL всех завершённых проверок и реестр
// ранних Put (T-147). Живёт одну задачу: алиасы — свойство пары
// субдоменов одного домена, между доменами итоговые URL не пересекаются
// (внешние парковки не считаются — повторное использование чужого тела
// подменяло бы TLS/заголовки чужого соединения). Потокобезопасен:
// проверки субдоменов идут параллельно.
type workerURLCache struct {
	mu    sync.Mutex
	pages map[string]*workerPage
	puts  []workerPut // ранние Put задачи в порядке скачиваний (T-147)
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

// addPut дописывает успешный ранний Put в реестр задачи (T-147).
func (c *workerURLCache) addPut(h content.Hash, created bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.puts = append(c.puts, workerPut{hash: h, created: created})
}

// takePuts возвращает накопленный реестр ранних Put (вызывается один раз
// в конце задачи — ProcessDomain переносит его в DomainResult).
func (c *workerURLCache) takePuts() []workerPut {
	c.mu.Lock()
	defer c.mu.Unlock()
	puts := c.puts
	c.puts = nil
	return puts
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
	// задачи (классика bare→www). Сеть не трогалась, но время ответа
	// первоисточника (до его заголовков) переносится в ResponseMS —
	// запись-оригинал алиас-класса несёт реальное время ответа страницы;
	// метрики времени ответа кэш-попадания по-прежнему не наблюдают.
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
	// последнего Fetch цепочки, ТЗ 5.5 п. 7 «время ответа»). Кэш-
	// попадание переносит измерение первоисточника (его Elapsed):
	// 0 — только у ошибок загрузки (заголовков не было).
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
	// ContentStored — тело страницы сохранено ранним Put в ContentStore
	// (T-147): хэш проверки — живая ссылка хранилища, кандидат на
	// content_hash записи. false — обрезанное тело (не сохраняется
	// вовсе), переполнение диска или лимит max_store_bytes (пауза
	// записи контента, ТЗ 8: результат домена не роняется). Само тело
	// в памяти не удерживается (T-147): байты — в хранилище, Get по
	// хэшу; пустое тело — валидный сохранённый контент (T-138).
	ContentStored bool
	// SPA — вердикт эвристики по телу (DetectSPA, T-057); nil при ошибке
	// загрузки. Публикацию q:render по нему делает рендер-конвейер
	// T-066 (renderq.go) в цепочке записи результата — при переполнении
	// очереди домен получает флаг render_pending и дорабатывается позже.
	SPA *httpfetch.SPAVerdict
	// ErrClass — класс ошибки (ErrNone — без ошибки; обрезка тела — ErrBody
	// при Available=true). ErrText — исходный текст для логов и разбора.
	ErrClass ErrorClass
	ErrText  string
	// storeErr — сбой раннего Put, не являющийся паузой записи контента
	// (T-147): странице не сопоставить ни «сохранено», ни «пропущено» —
	// эпизод честно падает (ProcessDomain возвращает ошибку, повторная
	// доставка, ТЗ 3.4). Поле внутреннее: заполняется checkScheme,
	// читается ProcessDomain; кэш-попадания его не наследуют (ошибка
	// относится к своей попытке записи).
	storeErr error
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
	// ContentStored — тело проверки-источника сохранено ранним Put
	// (T-147): у записей-оригиналов с хэшем без обрезки ссылка из Put
	// становится ссылкой строки subdomain_results (ContentSaver,
	// T-056). false у оригинала — контент не сохранён (переполнение
	// диска, лимит max_store_bytes): запись живёт с content_hash = nil
	// и ContentNotStored. Тело в записи не хранится (T-147) — байты
	// доступны хранилищем по хэшу; пустое тело — валидный контент
	// (T-138).
	ContentStored bool
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
	// puts — реестр ранних Put задачи (T-147, не для потребителя):
	// заполняется ProcessDomain, ContentSaver.Save сверяет его с
	// записями — ссылки, не ставшие строками, снимаются Delete.
	puts []workerPut
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
	// каждая задача держит в памяти результат домена — заголовки, хэши и
	// метаданные (тела страниц не удерживаются: ранний Put T-147),
	// читаемые тела ограничены PerDomain × max_body_bytes — типичная
	// задача единицы МиБ. Масштабирование нагрузки — инстансами (ТЗ 4.1
	// «масштабируется количеством горутин и процессов»), потолок
	// поднимается Options.Concurrency.
	//
	// Число — по замерам T-092 (docs/load-measurements.md §3, T-139):
	// средняя задача домена 1.67 с → потолок скользящего потребления
	// ≈ Concurrency/1.67с; 256 ≈ 150 дом/с — цель ТЗ §3.3 (первый цикл
	// 350 млн за недели, т.е. ≥100 дом/с) с запасом ×1.5 на деградацию
	// латентности от per-IP очередей (парковочные IP до 8% задач головы
	// очереди: per_ip_limit=4 держит подключения, задачи ждут семафора
	// в пределах total_timeout, слоты не освобождаются мгновенно —
	// head-of-line парковок съедает десятки слотов, барьер окна на таком
	// хвосте и резал этап до ≈2 дом/с). 256 × PerDomain 8 = 2048 ≈
	// http.concurrency 2000: окно задач заполняет транспортный бюджет,
	// не превышая. В памяти 256 задач держат типично десятки MiB тел
	// (по замеру ≈29 KiB сохранённого контента на домен, §7) — потолок
	// каждого тела остаётся max_body_bytes (BodyLimiter T-049). Боевой
	// экземпляр берёт значение из конфига http.task_concurrency
	// (cmd/http-worker), дефолт здесь — тот же, для изолированных
	// сборок и тестов.
	DefaultConcurrency = 256

	// DefaultConsume — записей q:http за одно чтение: размер пачки
	// XREADGROUP и буфера предвыборки скользящего потребления (T-139) —
	// чтение идёт вперёд обработки не глубже пары пачек (текущая в руке
	// читателя + буфер), следующее чтение происходит при освобождении
	// слотов задач (диспетчер забирает доставку из буфера, только когда
	// есть слот), окно чтения параллелизм не ограничивает.
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

// workerFailWaitBase — стартовая пауза предохранителя рабочего цикла
// (Run): доставки, подряд оставшиеся без XACK числом в целую пачку
// чтения (Consume, T-139) при систематическом сбое обработки —
// Options.OnResult/БД, — приостанавливают чтение q:http с удвоением
// паузы до DefaultErrorWait. Без паузы цикл продолжал бы черпать
// задачи: каждая доставка жгла попытку (XAUTOCLAIM после
// DefaultClaimMinIdle, T-025) и уносила её в q:dead. Переменная, а не
// константа: юнит-тесты сжимают паузу, не выжидая секунды.
var workerFailWaitBase = time.Second

// workerFailBackoff — пауза после streak подряд выдержанных пауз
// предохранителя (серия нарастает, пока читатель не увидит ни одного
// успеха — счётчик неудач обнуляется подтверждённой доставкой):
// удвоение от workerFailWaitBase, потолок DefaultErrorWait (сдвиг
// ограничен — переполнение Duration).
func workerFailBackoff(streak int) time.Duration {
	d := workerFailWaitBase << min(streak-1, 5)
	if d > DefaultErrorWait {
		d = DefaultErrorWait
	}
	return d
}

// Options — параметры воркера; нулевые значения заменяются дефолтами
// (withDefaults, стиль dnsworker.Options). Боевая сборка (cmd/http-worker)
// берёт Concurrency/Consume/PerDomain из конфигурации http.task_*
// (internal/config, T-139); здесь те же дефолты — для изолированных
// сборок и тестов.
type Options struct {
	// Concurrency — одновременных задач q:http в цикле Run; 0 —
	// DefaultConcurrency. Конфиг: http.task_concurrency.
	Concurrency int
	// Consume — записей q:http за одно чтение (пачка XREADGROUP и буфер
	// предвыборки скользящего потребления, T-139); 0 — DefaultConsume.
	// Конфиг: http.task_consume.
	Consume int
	// Group — группа потребителей q:http; "" — DefaultGroup.
	Group string
	// Consumer — имя потребителя группы; "" — "http-worker-<pid>".
	Consumer string
	// PerDomain — параллельных проверок (субдомен × протокол) внутри
	// задачи домена; 0 — DefaultPerDomain. Конфиг: http.task_per_domain.
	PerDomain int
	// Store — хранилище контента для раннего сохранения тел (T-147):
	// каждая скачанная страница уходит в Put по хэшу сразу по получении
	// и не удерживается в памяти слота (дедупликация алиасов идёт по
	// хэшам, T-055). Продакшн — *content.PackStore (тот же, что у
	// ContentSaver); nil — ошибка: без раннего Put тела записей
	// копились бы в памяти до конца задачи (≈3.34 МиБ/слот, замер
	// T-092 §11) — тихий откат оптимизации хуже явного отказа старта.
	Store content.ContentStore
	// MaxRedirects — лимит цепочки редиректов (http.max_redirects,
	// T-048): 0 — не следовать вовсе; отрицательные значения — дефолт
	// таблицы лимитов ТЗ 5.5 (httpfetch.DefaultMaxRedirects).
	MaxRedirects int
	// Recover — слой возврата зависших доставок (Reliab, T-025): Run
	// поднимает фоновый RecoverLoop (XAUTOCLAIM → та же обработка →
	// XACK) с теми же Group/Consumer; nil — восстановления нет
	// (изолированные сборки и тесты без Redis-специфики).
	Recover *queue.Reliab
	// RecoverBatch — пачка клейма RecoverLoop за проход; 0 — дефолт
	// очереди (queue.DefaultRecoverBatch). Боевая сборка передаёт
	// redis.recover_batch (T-153): темп возврата зависших — настраиваемая
	// величина очереди, а не окно потребления этапа.
	RecoverBatch int
	// RecoverInterval — период опроса зависших записей RecoverLoop;
	// 0 — дефолт очереди (queue.DefaultRecoverInterval). Конфиг —
	// redis.recover_interval (T-153).
	RecoverInterval time.Duration
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
	if opts.Store == nil {
		return nil, errors.New("httpworker: хранилище контента не задано (раннее сохранение тел, T-147)")
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

	// Сбой раннего Put, не являющийся паузой записи контента (ошибка
	// БД/ФС, отмена), роняет задачу (T-147): контент ссылки не получил,
	// молчание уронило бы его до перепроверки T-030 (месяцы); повторная
	// доставка переиграет эпизод целиком (ТЗ 3.4). Ссылки Put-ов этой
	// попытки, не снятые сверкой ContentSaver, остаются — утечка в
	// безопасную сторону (тот же класс, что крах между Put и фиксацией
	// строки у SwapRef, T-044).
	for i := range res.Subdomains {
		for _, c := range []*SchemeCheck{&res.Subdomains[i].HTTPS, &res.Subdomains[i].HTTP} {
			if c.storeErr != nil {
				return nil, fmt.Errorf("httpworker: домен %s: сохранение контента: %w", task.Name, c.storeErr)
			}
		}
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
				Variant:       vr.Variant,
				Scheme:        vr.Scheme,
				SubdomainID:   sub.SubdomainID,
				Label:         sub.Label,
				FinalURL:      src.FinalURL.String(),
				Host:          src.FinalURL.Hostname(),
				Path:          src.FinalURL.Path,
				HTTPStatus:    src.Status,
				Header:        src.Header,
				Cookies:       src.Cookies,
				TLS:           src.TLS,
				ResponseMS:    src.ResponseMS,
				Redirects:     src.Redirects,
				ContentHash:   src.ContentHash,
				Truncated:     src.Truncated,
				Charset:       src.Charset,
				SPA:           src.SPA,
				ContentStored: src.ContentStored,
				ErrClass:      src.ErrClass,
				AliasKind:     model.AliasNone,
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

	// Реестр ранних Put (T-147) — в результат: ContentSaver.Save сверит
	// его с записями (ссылки, не ставшие строками, снимутся Delete).
	// Тела проверок и записей к этому моменту в памяти уже нет (T-147):
	// ранний Put освободил их при получении.
	res.puts = cache.takePuts()
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
		check.ResponseMS = page.responseMS
		check.ContentHash = page.hash
		check.Truncated = page.truncated
		check.Charset = page.charset
		check.ContentStored = page.stored
		check.SPA = page.spa
		// Ошибка страницы (обрезка — ErrBody) переносится кэш-итогом:
		// запись того же контента несёт тот же класс, что и у
		// первоисточника (иначе subdomain_results.error расходился бы
		// от попадания в кэш, метрики недосчитали бы ошибку).
		check.ErrClass = page.errClass
		check.ErrText = page.errText
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
	body, readErr = workerReadBody(res)
	if res.BodyLimiter != nil {
		truncated = res.BodyLimiter.Truncated()
	} else {
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
	spa := httpfetch.DetectSPA(cs.UTF8)
	check.SPA = &spa

	// Раннее сохранение тела (T-147): байты уходят в хранилище сразу
	// по получении — дедупликация алиасов (T-055) идёт по хэшам, слот
	// не удерживает тела до конца задачи. Обрезанное тело не
	// сохраняется вовсе (хэш усечённых байт врал бы при дедупе,
	// T-049); ErrStoreFull/ErrTooLarge — пауза записи контента (ТЗ 8):
	// страница помечается несохранённой, результат домена не роняется;
	// прочие ошибки — storeErr, эпизод упадёт в ProcessDomain.
	if !truncated {
		pr, err := w.opt.Store.Put(ctx, cs.UTF8)
		switch {
		case err == nil:
			if pr.Hash != hash {
				check.storeErr = fmt.Errorf("хранилище вернуло хэш %s, воркер вычислил %s (контракт HashOf, T-041)",
					pr.Hash, hash)
				return check
			}
			check.ContentStored = true
			cache.addPut(hash, pr.Created)
		case errors.Is(err, content.ErrStoreFull), errors.Is(err, content.ErrTooLarge):
			// Контент не сохранён, запись результата живёт с
			// content_hash = nil (нормализует ContentSaver, ТЗ 8).
			w.opt.log().Warn("контент страницы не сохранён — запись будет без content_hash",
				"url", res.FinalURL.String(), "err", err.Error())
		default:
			check.storeErr = err
			return check
		}
	}

	// Итоговый URL — в кэш задачи: следующие проверки, начинающиеся на
	// нём (www после редиректа bare), страницы повторно не скачивают.
	cache.put(workerCacheKey(res.FinalURL), &workerPage{
		final:      res.FinalURL,
		status:     check.Status,
		header:     check.Header,
		cookies:    check.Cookies,
		tls:        check.TLS,
		hash:       check.ContentHash,
		stored:     check.ContentStored,
		charset:    check.Charset,
		truncated:  check.Truncated,
		responseMS: check.ResponseMS,
		spa:        check.SPA,
		errClass:   check.ErrClass,
		errText:    check.ErrText,
	})
	return check
}

// workerPreallocMax — потолок предвыделения буфера чтения тела по
// Content-Length (T-147): враньё сервера (гигабайтный Content-Length) не
// резервирует гигабайт — такие тела читаются обычным ростом буфера.
const workerPreallocMax = workerBodyHardLimit

// workerReadBody читает тело ответа одним буфером с предвыделением по
// Content-Length (T-147): io.ReadAll растит буфер от 512 байт удвоением
// — на телах в десятки–сотни КиБ это два-три поколения копий на страницу
// (× PerDomain одновременных проверок × Concurrency слотов — транзитный
// мусор GC, входивший в пик памяти слота). Content-Length честен только
// без Content-Encoding (транспорт T-048/ТЗ 5.5 отдаёт уже распакованный
// поток, заголовок описывает сжатые байты): в этом случае буфер
// выделяется ровно по заявленному размеру и наполняется io.ReadFull;
// недосланное и пересланное сервером, сжатые ответы и кривой/отсутствующий
// заголовок — обычное чтение с ростом. Без BodyLimiter чтение ограничено
// страховочным потолком воркера (workerBodyHardLimit) — предвыделение
// его не подменяет.
func workerReadBody(res *httpfetch.FetchResult) ([]byte, error) {
	rc := io.Reader(res.Body)
	if res.BodyLimiter == nil {
		rc = io.LimitReader(res.Body, workerBodyHardLimit+1)
	}
	if res.Header.Get("Content-Encoding") == "" {
		if n, err := strconv.ParseInt(res.Header.Get("Content-Length"), 10, 64); err == nil && n > 0 && n <= workerPreallocMax {
			buf := make([]byte, n)
			r, err := io.ReadFull(rc, buf)
			if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
				return nil, err
			}
			// Хвост за заявленным размером (сервер наврал в большую
			// сторону) дочитывается ростом; у честного ответа пусто.
			extra, err := io.ReadAll(rc)
			if err != nil {
				return nil, err
			}
			return append(buf[:r], extra...), nil
		}
	}
	return io.ReadAll(rc)
}

// --- рабочий цикл ------------------------------------------------------------

// Run — рабочий цикл http-worker (ТЗ 4.2, §5.5): читает задачи q:http
// группой потребителей пачками Consume и обрабатывает доставки параллельно
// до Concurrency доменов, каждую — паттерном Processing (T-025): обработка
// (ProcessDomain + Options.OnResult) → XACK. Потребление скользящее
// (T-139): следующее чтение происходит при освобождении слотов задач
// (чтение идёт вперёд обработки не глубже пары пачек Consume), цикл не
// ждёт завершения пачки целиком — барьер окна на хвосте длительностей
// (тарпиты до total_timeout, 20–25 с) удерживал пропускную способность
// этапа на уровне ≈2 дом/с при простаивающем транспорте (замер T-092).
// Работает до отмены ctx и возвращает nil — контракт OnRun каркаса
// (T-012: начатые задачи дорабатывают или возвращаются очереди повторной
// доставкой). Ошибка чтения или обработки не останавливает цикл:
// логируется (Warn), неподтверждённая доставка вернётся XAUTOCLAIM
// (ТЗ 4.2); Consume доставок подряд без единого подтверждения —
// систематический сбой обработки — выдерживает паузу-предохранитель
// (workerFailWaitBase): черпать новые задачи при лежащей обработке
// бессмысленно, попытки доставок сгорают.
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

	// Возврат зависших доставок (T-025): фоновый RecoverLoop от имени
	// того же потребителя — клейм XAUTOCLAIM и та же обработка задач.
	// Живёт до отмены ctx; Run дожидается его выхода (начатая задача
	// дорабатывает или остаётся неподтверждённой, ТЗ 3.4).
	if o.Recover != nil {
		recDone := make(chan struct{})
		go func() {
			defer close(recDone)
			_ = queue.RecoverLoop(ctx, o.Recover, func(c context.Context, del queue.Delivery) error {
				return w.processDelivery(c, del, o)
			}, queue.RecoverOptions{
				Stream:   queue.StreamHTTP,
				Group:    o.Group,
				Consumer: o.Consumer,
				// Темп обхода PEL — очередь, не этап (redis.recover_*,
				// T-153); нули — дефолты очереди.
				Batch:    o.RecoverBatch,
				Interval: o.RecoverInterval,
				Log:      log,
			})
		}()
		defer func() { <-recDone }()
	}

	sem := make(chan struct{}, o.Concurrency)
	// Скользящее потребление (T-139): читатель держит впереди обработки
	// не больше пары пачек Consume доставок (текущая пачка в руке +
	// буфер pending), диспетчер ниже занимает слот семафора на время
	// одной доставки и сразу берёт следующую — завершение задачи
	// освобождает слот и двигает чтение, никто не ждёт пачку целиком.
	// Прочитанные, но не начатые доставки (буфер полон, слоты заняты
	// хвостом длительностей) на отмене ctx остаются без XACK и
	// возвращаются очереди клеймом XAUTOCLAIM — как и не начатые
	// доставки окна прежде (at-least-once, ТЗ 3.4).
	pending := make(chan queue.Delivery, o.Consume)
	var failed atomic.Int64 // доставок подряд без подтверждения
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		failStreak := 0 // подряд выдержанных пауз предохранителя
		for {
			// Предохранитель (workerFailWaitBase): доставки, подряд
			// оставшиеся без подтверждения числом в целую пачку чтения
			// (Consume) без единого успеха — систематический сбой
			// обработки (Options.OnResult/БД); пауза с удвоением до
			// DefaultErrorWait. Подтверждённая доставка обнуляет счётчик
			// неудач сразу, серия пауз сбрасывается на следующей
			// итерации читателя, увидевшей счётчик нулевым.
			if n := failed.Load(); n == 0 {
				failStreak = 0
			} else if int(n) >= o.Consume {
				if ctx.Err() != nil {
					return // graceful shutdown: паузы не было и не будет
				}
				failStreak++
				pause := workerFailBackoff(failStreak)
				// Наблюдаемость простоя (ревью T-139): пауза до
				// DefaultErrorWait останавливает чтение всего этапа —
				// Warn с серией и числом неудач (частота ограничена
				// удвоением паузы, не чаще ~2/мин на серии).
				log.Warn("пауза предохранителя чтения q:http: доставки падают подряд",
					"failed", n, "streak", failStreak, "wait", pause.String())
				wait(pause)
				if ctx.Err() != nil {
					return // graceful shutdown: wait вышел по отмене
				}
			}
			deliveries, err := q.Consume(ctx, queue.StreamHTTP, o.Group, o.Consumer, o.Consume)
			if err != nil {
				if ctx.Err() != nil {
					return // graceful shutdown
				}
				log.Warn("цикл http-worker: чтение q:http", "err", err)
				wait(DefaultErrorWait)
				continue
			}
			for _, d := range deliveries {
				select {
				case pending <- d:
				case <-ctx.Done():
					return // буферные доставки без XACK вернёт XAUTOCLAIM
				}
			}
		}
	}()

	// Диспетчер: ждёт свободный слот (или отмену) и запускает доставку.
	// Слот семафора — с выходом по отмене: отмена ctx во время ожидания
	// слота не должна запускать доставку с уже отменённым ctx (выбор
	// канала select недетерминирован, поэтому отмена проверяется и после
	// получения слота).
	var wg sync.WaitGroup
dispatch:
	for {
		var d queue.Delivery
		select {
		case <-ctx.Done():
			break dispatch // доставки буфера не начаты — без XACK
		case d = <-pending:
		}
		select {
		case <-ctx.Done():
			break dispatch
		case sem <- struct{}{}:
		}
		if ctx.Err() != nil {
			<-sem
			break dispatch
		}
		wg.Add(1)
		go func(d queue.Delivery) {
			defer wg.Done()
			defer func() { <-sem }()
			err := queue.Processing(ctx, q, o.Group, d, func(c context.Context, del queue.Delivery) error {
				return w.processDelivery(c, del, o)
			})
			if err != nil {
				failed.Add(1)
				if ctx.Err() == nil {
					log.Warn("задача q:http не подтверждена — вернётся повторной доставкой",
						"entry", d.ID, "err", err)
				}
			} else {
				failed.Store(0)
			}
		}(d)
	}
	wg.Wait()    // начатые доставки дорабатывают (graceful stop, ТЗ 3.4)
	<-readerDone // читатель завершается той же отменой ctx (Consume/wait)
	return nil
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
// ipv6 — и AAAA, опрос типов ОДНОВРЕМЕННЫЙ (T-101: резолв хоста
// редиректа сидит в критическом пути подключения цепочки T-048 —
// латентность max(A, AAAA), не сумма); IP-литералы (Location вида
// http://192.0.2.1/) проходят без обмена. CNAME разворачивает сам
// рекурсор (unbound, ТЗ 5.4) — в ответе на A лежат и записи цели
// (RFC 1034 §4.3.2, T-037). Отрицание (NXDOMAIN) и пустой ответ —
// ошибка: подключаться не к чему. Сбой или отказ одного типа не роняет
// резолв, если другой дал адреса: транзиентный отказ AAAA не должен
// ломать подключение по рабочему A (NODATA — не здесь: NOERROR без
// записей просто ничего не добавляет); собранного нет вовсе —
// возвращается первая по порядку типов ошибка обмена (A раньше AAAA —
// детерминизм выбора, как при прежнем последовательном опросе).
// Отмена ctx — общая для обоих обменов: Exchange получает тот же ctx,
// отменённый резолв возвращается его ошибкой.
func (d *DNSLookup) LookupNetIP(ctx context.Context, host string) ([]netip.Addr, error) {
	if ip, err := netip.ParseAddr(host); err == nil {
		return []netip.Addr{ip}, nil
	}
	types := []uint16{dns.TypeA}
	if d.ipv6 {
		types = append(types, dns.TypeAAAA)
	}
	// Обмены идут параллельно с общим ctx, но собираются по порядку
	// types — итог стабилен: адреса A, за ними AAAA, независимо от того,
	// какой тип ответил раньше (транспорт T-047 видит тот же порядок,
	// что и при последовательном опросе). Горутины пишут в разные
	// ячейки answers — гонки нет; WaitGroup, а не errgroup: ошибки
	// типов не отменяют соседа (семантика HW-07), ошибка — не сигнал.
	answers := make([]workerDNSAnswer, len(types))
	if len(types) == 1 {
		// dns.ipv6=false — один обмен, без горутин: параллелить нечего.
		answers[0] = d.lookupType(ctx, host, types[0])
	} else {
		var wg sync.WaitGroup
		for i, t := range types {
			wg.Add(1)
			go func(i int, t uint16) {
				defer wg.Done()
				answers[i] = d.lookupType(ctx, host, t)
			}(i, t)
		}
		wg.Wait()
	}
	var (
		out      []netip.Addr
		firstErr error // первая по порядку типов ошибка — итог, если адресов нет
	)
	for i := range answers {
		if answers[i].err != nil {
			if firstErr == nil {
				firstErr = answers[i].err
			}
			continue // другой тип ещё может дать адреса
		}
		out = append(out, answers[i].addrs...)
	}
	if len(out) == 0 {
		if firstErr != nil {
			return nil, firstErr
		}
		return nil, fmt.Errorf("httpworker: резолв %q: пустой ответ", host)
	}
	return out, nil
}

// workerDNSAnswer — итог одного обмена A/AAAA: адреса этого типа или
// ошибка (сбой обмена, отказ RCode); пустые адреса без ошибки — NOERROR
// без записей (NODATA), резолву они ничего не добавляют.
type workerDNSAnswer struct {
	addrs []netip.Addr
	err   error
}

// lookupType — один обмен по типу записи (общий ctx с соседним типом).
func (d *DNSLookup) lookupType(ctx context.Context, host string, t uint16) workerDNSAnswer {
	res, err := d.resolve.Exchange(ctx, dnsclient.Question{Name: host, Type: t})
	switch {
	case err != nil:
		return workerDNSAnswer{err: fmt.Errorf("httpworker: резолв %q %s: %w", host, dns.TypeToString[t], err)}
	case res.RCode != dnsclient.RCodeNOERROR:
		return workerDNSAnswer{err: fmt.Errorf("httpworker: резолв %q %s: %s", host, dns.TypeToString[t], res.RCode.String())}
	}
	var addrs []netip.Addr
	for _, rr := range res.RecordsOf(t) {
		switch r := rr.(type) {
		case *dns.A:
			if addr, ok := netip.AddrFromSlice(r.A.To4()); ok {
				addrs = append(addrs, addr)
			}
		case *dns.AAAA:
			if addr, ok := netip.AddrFromSlice(r.AAAA.To16()); ok {
				addrs = append(addrs, addr.Unmap())
			}
		}
	}
	return workerDNSAnswer{addrs: addrs}
}

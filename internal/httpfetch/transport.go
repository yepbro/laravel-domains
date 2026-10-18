// Package httpfetch — HTTP-транспорт этапа проверки сайтов (ТЗ 5.5,
// пп. 1–2 и таблица лимитов).
//
// Транспорт решает три задачи, недоступные дефолтному net/http:
//
//   - подключение к заранее резолвленному IP: адреса основного хоста
//     приходят из DNS-этапа в задаче q:http (ТЗ 5.5 п. 2), запрос задаёт
//     хост (Host-заголовок, SNI) и адреса подключения раздельно;
//   - послойные таймауты из таблицы лимитов ТЗ 5.5: connect 5 с,
//     TLS handshake 5 с, ожидание заголовков 10 с, общий таймаут
//     запроса 20 с — из секции http конфигурации (T-006);
//   - резолв хостов HTTP-редиректов «через тот же резолвер с кэшем»
//     (ТЗ 5.5 п. 2): подключение к хосту, для которого нет адресов
//     DNS-этапа (редирект на другой хост), идёт через Resolver
//     транспорта; обёртка-кэш — CachingResolver;
//   - лимиты и защита T-049 (таблица лимитов ТЗ 5.5): потолок тела с
//     признаком обрезки, анти-тарпит (мин. скорость после первого
//     байта), соединения на один IP, глобальные соединения и RPS —
//     limits.go, включаются Options.Limits.
//
// Этика и минимум запросов (ТЗ 5.5, §9): краулер запрашивает только
// главную страницу «/» — FetchRequest для проверки сайта строится из
// схемы и хоста без пути (или с путём «/», это один и тот же запрос).
// Код пакета не обращается к /robots.txt и не придумывает путей сам:
// иные пути в цепочке редиректов (T-048) задаются ответами сервера —
// Location и meta refresh, то есть инициатива сайта, а не краулера.
// User-Agent каждого запроса — честный, с контактами оператора:
// http.user_agent конфигурации, дефолт config.DefaultUserAgent
// (T-052). «Один запрос на (субдомен, вариант) без повторов» —
// общий кэш итоговых URL воркера (T-053), транспорт повторов не
// делает: один Fetch — один исходящий запрос (плюс ходы цепочки по
// Location сервера).
//
// Редиректы транспорт не обрабатывает: один запрос — один ответ,
// Location остаётся заголовком ответа; следование по цепочке
// (max_redirects, детект циклов, meta refresh, punycode-нормализация
// хостов) строит T-048 поверх Fetch или поверх RoundTripper-интерфейса:
// http.Client с этим транспортом резолвит ходы редиректов через
// Resolver автоматически (исходный ctx не переносит адреса DNS-этапа
// на новый хост — см. dialContext).
package httpfetch

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"sync"
	"time"

	"crawler/internal/config"
)

// Значения по умолчанию — таблица лимитов ТЗ 5.5 («все настраиваются»).
const (
	// DefaultConnectTimeout — установка TCP-соединения (connect 5 с).
	DefaultConnectTimeout = 5 * time.Second
	// DefaultTLSHandshakeTimeout — TLS-рукопожатие (5 с).
	DefaultTLSHandshakeTimeout = 5 * time.Second
	// DefaultHeaderTimeout — ожидание заголовков ответа (10 с);
	// в транспорте это ResponseHeaderTimeout, тело им не ограничено.
	DefaultHeaderTimeout = 10 * time.Second
	// DefaultTotalTimeout — общий таймаут запроса (20 с): контекст
	// запроса покрывает установку соединения, TLS, отправку и чтение
	// тела; завершается закрытием Body результата.
	DefaultTotalTimeout = 20 * time.Second
	// DefaultFallbackDelay — пауза перед стартом попытки подключения к
	// очередному адресу (RFC 8305 connection attempt delay; 300 мс — как
	// FallbackDelay у net.Dialer).
	DefaultFallbackDelay = 300 * time.Millisecond
)

// Options — параметры транспорта. Нулевые и отрицательные значения
// заменяются дефолтами таблицы лимитов ТЗ 5.5 (репо-стиль
// dnsclient.Options: «0 — значение по умолчанию»).
type Options struct {
	// ConnectTimeout — таймаут установки TCP-соединения к одному адресу.
	ConnectTimeout time.Duration
	// TLSHandshakeTimeout — таймаут TLS-рукопожатия.
	TLSHandshakeTimeout time.Duration
	// HeaderTimeout — ожидание заголовков ответа после отправки запроса.
	HeaderTimeout time.Duration
	// TotalTimeout — общий таймаут Fetch: соединение, TLS, заголовки и
	// чтение тела (действует, пока не закрыт Body результата).
	TotalTimeout time.Duration
	// FallbackDelay — пауза между попытками подключения к очередному
	// адресу из нескольких (см. dialAddrs).
	FallbackDelay time.Duration
	// MaxBodyBytes — предел читаемого тела («макс. размер читаемого
	// тела | 2 МБ», ТЗ 5.5). Применяет лимитёр T-049 (Options.Limits:
	// обрезка с признаком BodyLimiter.Truncated); без лимитёра
	// значение переносится из конфигурации к потребителю как есть.
	// С включёнными Limits потолок тела один на транспорт и лимитёр —
	// Limits.MaxBodyBytes (bodyCeiling), это поле на распаковку
	// не влияет.
	MaxBodyBytes int64
	// Limits — лимиты и защита T-049 (таблица лимитов ТЗ 5.5): потолок
	// тела, анти-тарпит, соединения на один IP, глобальные соединения
	// и RPS. nil — транспорт без лимитов (изолированные тесты слоёв
	// T-047); NewOptions включает лимиты из секции http конфигурации.
	Limits *LimitOptions
	// UserAgent — User-Agent всех запросов Fetch: честный, с
	// контактами оператора (ТЗ 5.5, §9; поле http.user_agent
	// конфигурации, дефолт — config.DefaultUserAgent, формализация —
	// T-052). Пустой — не ставится, и транспорт посылает UA Go по
	// умолчанию: лазейка только для изолированных тестов слоёв;
	// NewOptions из секции http подставляет дефолт при пустом поле —
	// боевой транспорт всегда представляется.
	UserAgent string
	// Resolver — резолв хостов без адресов DNS-этапа (редиректы на
	// другие хосты). nil — системный резолвер (SystemResolver).
	// Кэш — обёртка NewCachingResolver поверх резолвера этапа; «тот же
	// резолвер» ТЗ 5.5 п. 2 — адаптер dnsclient соберёт wiring воркера.
	Resolver Resolver
	// TLSClientConfig — конфигурация TLS-клиента. SNI и проверка
	// сертификата выполняются по хосту URL (DialContext подключается к
	// заранее резолвенному IP, не меняя целевой адрес). nil — обычная
	// проверка; InsecureSkipVerify и сохранение результата проверки
	// сертификата (издатель, срок, SAN) — T-050.
	TLSClientConfig *tls.Config
}

// NewOptions собирает параметры из секции http конфигурации (T-006);
// лимиты T-049 включаются всегда — боевой транспорт не бывает без
// лимитов таблицы ТЗ 5.5. FallbackDelay, Resolver и TLSClientConfig в
// ТЗ числами не заданы: пауза гонки адресов — транспортная деталь,
// резолвер этапа и TLS подключаются при сборке воркера (T-050/T-053).
// Пустой UserAgent (конфиг собран руками, без Default/Validate)
// подставляется дефолтом config.DefaultUserAgent — транспорт из
// конфигурации не бывает без честного UA (ТЗ §9, T-052).
func NewOptions(cfg config.HTTP) Options {
	limits := NewLimitOptions(cfg)
	ua := cfg.UserAgent
	if strings.TrimSpace(ua) == "" {
		ua = config.DefaultUserAgent
	}
	return Options{
		ConnectTimeout:      cfg.ConnectTimeout.D(),
		TLSHandshakeTimeout: cfg.TLSHandshakeTimeout.D(),
		HeaderTimeout:       cfg.HeaderTimeout.D(),
		TotalTimeout:        cfg.TotalTimeout.D(),
		MaxBodyBytes:        cfg.MaxBodyBytes.B(),
		UserAgent:           ua,
		Limits:              &limits,
	}
}

// withDefaults заменяет нулевые и отрицательные значения дефолтами
// таблицы лимитов ТЗ 5.5.
func (o Options) withDefaults() Options {
	if o.ConnectTimeout <= 0 {
		o.ConnectTimeout = DefaultConnectTimeout
	}
	if o.TLSHandshakeTimeout <= 0 {
		o.TLSHandshakeTimeout = DefaultTLSHandshakeTimeout
	}
	if o.HeaderTimeout <= 0 {
		o.HeaderTimeout = DefaultHeaderTimeout
	}
	if o.TotalTimeout <= 0 {
		o.TotalTimeout = DefaultTotalTimeout
	}
	if o.FallbackDelay <= 0 {
		o.FallbackDelay = DefaultFallbackDelay
	}
	return o
}

// FetchRequest — один HTTP-запрос: URL и заранее резолвленные адреса
// хоста из DNS-этапа (ТЗ 5.5 п. 2; формат задачи q:http — T-024).
// Для проверки сайта URL строится из схемы и хоста с путём «/» или
// без пути вовсе — это один и тот же запрос (RequestURI даёт «/»);
// иные пути транспорту не нужны (ТЗ 5.5 п. 4, §9: только главная
// страница, /robots.txt не запрашивается), пути внутри цепочки
// редиректов задаёт сервер. Addrs обязательны: транспорт не резолвит
// основной хост сам — этим он и отличается от обычного клиента;
// запрос задаёт хост (Host-заголовок, SNI) и адреса подключения
// раздельно.
type FetchRequest struct {
	URL   *url.URL
	Addrs []netip.Addr
}

// FetchResult — итог одного запроса для потребителей этапа. Минимальный
// набор T-047: расширяют следующие задачи (лимит тела и тарпиты — T-049,
// атрибуты сертификата — T-050, кодировки — T-051, цепочка редиректов —
// T-048/T-058).
type FetchResult struct {
	// StatusCode — код статуса ответа (3xx с Location — данные для
	// редирект-цикла T-048, не ошибка).
	StatusCode int
	// Status — строка статуса целиком ("200 OK").
	Status string
	// Header — заголовки ответа.
	Header http.Header
	// Body — тело ответа. Обязательно закрывается вызывающим: Close
	// освобождает соединение и останавливает общий таймаут запроса.
	// С лимитами T-049 (Options.Limits) тело дополнительно обёрнуто в
	// BodyLimiter: потолок MaxBodyBytes и анти-тарпит (ТЗ 5.5).
	Body io.ReadCloser
	// BodyLimiter — лимитёр тела T-049; nil — лимиты выключены и Body
	// без обёртки. Truncated — признак «тело обрезано потолком
	// MaxBodyBytes»: данные частичны, не ошибка сети (ТЗ 8), решение —
	// за потребителем (T-058). Признак выставляется чтением Body к
	// моменту io.EOF.
	BodyLimiter *BodyLimiter
	// FinalURL — URL этого ответа: у одного запроса совпадает с
	// запрошенным; в цепочке редиректов T-048 подставит адрес
	// последнего ответа.
	FinalURL *url.URL
	// Elapsed — время до заголовков ответа (время ответа ТЗ 5.5 п. 7).
	Elapsed time.Duration
	// TLS — параметры установленного TLS-соединения (nil для http).
	TLS *tls.ConnectionState
	// TLSInfo — разбор сертификата соединения и результат
	// самостоятельной проверки (ТЗ 5.5 п. 3): издатель, субъект,
	// серийный номер, срок действия, SAN, цепочка, valid+причины.
	// Заполняется Fetch — хост URL (SNI/Host) для проверки известен
	// там, а не в ConnectionState; nil для http (не ошибка, §8).
	// Пользователи RoundTrip напрямую собирают сами (NewTLSInfo).
	TLSInfo *TLSInfo
}

// cancelBody — тело ответа, остановка общего таймаута при Close:
// контекст TotalTimeout живёт всё чтение тела, а не только Fetch
// (преждевременная отмена убивала бы соединение на первом же байте).
type cancelBody struct {
	rc     io.ReadCloser
	cancel context.CancelFunc
}

func (b *cancelBody) Read(p []byte) (int, error) { return b.rc.Read(p) }

func (b *cancelBody) Close() error {
	err := b.rc.Close()
	b.cancel()
	return err
}

// dialFunc — одна попытка TCP-подключения к адресу. Отдельный тип —
// шов подмены транспорта в юнит-тестах без сети (стиль
// dnsclient.exchangeFunc); продакшн-реализация — dialNet.
type dialFunc func(ctx context.Context, addr netip.Addr, port string) (net.Conn, error)

// Fetcher — HTTP-транспорт этапа. Реализует http.RoundTripper (один
// запрос-ответ) и добавляет Fetch — обёртку одного запроса с адресами
// DNS-этапа и общим таймаутом. Безопасен для одновременного
// использования из горутин: разделяемое состояние — http.Transport и
// Resolver, оба потокобезопасны.
type Fetcher struct {
	opt  Options
	rt   *http.Transport
	res  Resolver
	dial dialFunc
	// lim — активные лимиты T-049 (Options.Limits); nil — выключены.
	lim *limitState
}

// compile-time check: *Fetcher — http.RoundTripper.
var _ http.RoundTripper = (*Fetcher)(nil)

// NewFetcher создаёт транспорт из опций (нулевые значения — дефолты
// таблицы лимитов ТЗ 5.5). Resolver nil — системный резолвер:
// локальная разработка и юнит-тесты без DNS-этапа.
func NewFetcher(opts Options) *Fetcher {
	opts = opts.withDefaults()
	if opts.Resolver == nil {
		opts.Resolver = SystemResolver
	}
	f := &Fetcher{
		opt: opts,
		res: opts.Resolver,
	}
	f.dial = f.dialNet
	if opts.Limits != nil {
		f.lim = newLimitState(opts.Limits.withDefaults())
	}
	f.rt = &http.Transport{
		// Прямые подключения: исходящие соединения одного IP краулера
		// (ТЗ 10.2), прокси не настраиваются.
		Proxy: nil,
		// Подключение к заранее резолвенному IP: адреса основного хоста
		// приходят в ctx запроса Fetch, хосты редиректов резолвит
		// dialContext через Resolver.
		DialContext: f.dialContext,
		// ТЗ 5.5: keep-alive «отключён или очень короткий idle». Выбрано
		// «отключён» — зафиксировано в T-049: запрашивается одна главная
		// страница на сайт (минимум запросов), цепочка редиректов идёт
		// по разным хостам, а пул по ключу «хост» переживал бы смену
		// адреса при перерезолве и держал тысячи неактивных соединений
		// при concurrency 2000. IdleConnTimeout при DisableKeepAlives
		// эффекта не имеет (соединения не живут дольше ответа) —
		// «короткий idle» не настраивается.
		DisableKeepAlives: true,
		// Послойные таймауты (таблица лимитов ТЗ 5.5): рукопожатие TLS
		// и ожидание заголовков; connect и общий — в диале и Fetch.
		TLSHandshakeTimeout:   opts.TLSHandshakeTimeout,
		ResponseHeaderTimeout: opts.HeaderTimeout,
		TLSClientConfig:       opts.TLSClientConfig,
	}
	return f
}

// CloseIdleConnections закрывает неактивные соединения транспорта.
// С DisableKeepAlives их не бывает; метод — контракт гигиены остановки
// для сборки воркера и тестов.
func (f *Fetcher) CloseIdleConnections() { f.rt.CloseIdleConnections() }

// RoundTrip реализует http.RoundTripper: одиночный запрос-ответ без
// следования редиректам (это уровень http.Client; цепочка — T-048).
// Запрос к хосту, для которого в ctx нет адресов DNS-этапа (редирект),
// подключается через Resolver транспорта — http.Client поверх Fetcher
// получает резолв хостов редиректов без дополнительного кода.
//
// Здесь же — сжатие (T-051, ТЗ 5.5): запросу ставится Accept-Encoding
// (gzip, br), ответ распаковывается с потолком. Хук в RoundTrip, а не
// только в Fetch: путь http.Client поверх Fetcher (цепочка редиректов
// T-048) идёт через этот метод и получает то же поведение. Запрос
// клонируется — контракт RoundTrip запрещает менять чужой запрос, а
// переиспользуемые запросы гоняются по заголовкам конкурентно.
func (f *Fetcher) RoundTrip(req *http.Request) (*http.Response, error) {
	// Токен темпа запросов (ТЗ 5.5: RPS): через этот RoundTrip идут оба
	// пути — и Fetch, и http.Client поверх Fetcher (цепочка редиректов
	// T-048) — токен берётся один раз на исходящий запрос.
	if f.lim != nil {
		if err := f.lim.rps.take(req.Context()); err != nil {
			return nil, fmt.Errorf("httpfetch: %s %s: %w", req.Method, req.URL, err)
		}
	}
	req = req.Clone(req.Context())
	if req.Header.Get("Accept-Encoding") == "" {
		req.Header.Set("Accept-Encoding", acceptEncoding)
	}
	resp, err := f.rt.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	if err := f.decodeResponseBody(resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// bodyCeiling — единый источник потолка тела для распаковки и лимита
// чтения: при включённых лимитах T-049 — Limits.MaxBodyBytes (то же
// число, по которому режет BodyLimiter), без лимитов — Options.MaxBodyBytes
// либо дефолт таблицы лимитов. Два раздельных поля (Options и Limits)
// в боевой сборке NewOptions заполняются одним cfg.MaxBodyBytes, но
// ручная сборка Options расходилась — распаковка и чтение видели
// разные потолки.
func (f *Fetcher) bodyCeiling() int64 {
	if f.lim != nil && f.lim.opt.MaxBodyBytes > 0 {
		return f.lim.opt.MaxBodyBytes
	}
	if f.opt.MaxBodyBytes > 0 {
		return f.opt.MaxBodyBytes
	}
	return defaultDecompressLimit
}

// decodeResponseBody распаковывает тело ответа по Content-Encoding с
// потолком распакованного размера (защита от zip-бомб, ТЗ 5.5) и
// вписывается в цепочку чтения: сеть → gunzip/brotli (потолок
// bodyCeiling) → лимит читаемого тела T-049 → потребитель. Потолок
// равен лимиту тела: распакованный поток и есть тело, иначе чтение до
// конца (хэш дедупа) раздуло бы бомбу до лимита, но гигабайтами
// распаковки. С лимитёром T-049 потолок пропускает ещё один пробный
// байт за пределом (лимит+1): лимитёр, исчерпав лимит, читает его для
// классификации хвоста, и сжатое тело чуть больше лимита даёт ту же
// обрезку с Truncated, что и несжатое, — бомба при этом дальше
// лимита+1 не читается. Заголовки Content-Encoding/Content-Length
// снимаются: тело в руках уже распаковано, кодирующие заголовки лгали
// бы (как у прозрачной распаковки net/http). Неподдерживаемая
// кодировка — ошибка запроса: молча отдать сжатые байты телом хуже
// честного отказа.
func (f *Fetcher) decodeResponseBody(resp *http.Response) error {
	ce := resp.Header.Get("Content-Encoding")
	if ce == "" {
		return nil // сжатия нет — тело как есть
	}
	limit := f.bodyCeiling()
	if f.lim != nil {
		// Выше распаковки стоит BodyLimiter: его пробный байт за
		// пределом лимита проходит сквозь распаковку (см. комментарий
		// функции). Без +1 байт натыкался на потолок распаковки и
		// превращал обрезку в ErrDecompressTooLarge — read-ошибку,
		// по которой воркер гасит Available живого сайта.
		limit++
	}
	body, err := decodeContentEncoding(resp.Body, ce, limit)
	if err != nil {
		_ = resp.Body.Close()
		return err
	}
	resp.Body = body
	if strings.ToLower(strings.TrimSpace(ce)) != "identity" {
		// Реальная распаковка: кодирующие заголовки лгали бы про тело
		// в руках — снимаем (как прозрачная распаковка net/http);
		// при identity оба остаются правдой.
		resp.Header.Del("Content-Encoding")
		resp.Header.Del("Content-Length")
	}
	return nil
}

// Fetch выполняет GET одного URL с подключением к заранее резолвленным
// адресам (ТЗ 5.5 пп. 1–2): хост URL задаёт Host-заголовок и SNI,
// подключение идёт к Addrs. Общий таймаут (TotalTimeout, по умолчанию
// 20 с) покрывает запрос целиком, включая чтение тела вызывающим —
// до закрытия Body. Ошибочный статус ответа — не ошибка Fetch (код в
// FetchResult); error — только транспорт (сеть, таймауты, отмена ctx,
// неподдерживаемое Content-Encoding — см. RoundTrip).
func (f *Fetcher) Fetch(ctx context.Context, r FetchRequest) (*FetchResult, error) {
	if r.URL == nil {
		return nil, errors.New("httpfetch: запрос без URL")
	}
	if r.URL.Scheme != "http" && r.URL.Scheme != "https" {
		return nil, fmt.Errorf("httpfetch: неподдерживаемая схема %q (ожидается http/https)", r.URL.Scheme)
	}
	host := strings.ToLower(r.URL.Hostname())
	if host == "" {
		return nil, fmt.Errorf("httpfetch: URL %s без хоста", r.URL)
	}
	if len(r.Addrs) == 0 {
		return nil, fmt.Errorf("httpfetch: %s: нет адресов DNS-этапа (ТЗ 5.5 п. 2)", r.URL)
	}

	tctx, cancel := context.WithTimeout(ctx, f.opt.TotalTimeout)
	tctx = context.WithValue(tctx, addrsKey{}, &addrsValue{host: host, addrs: r.Addrs})
	req, err := http.NewRequestWithContext(tctx, http.MethodGet, r.URL.String(), nil)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("httpfetch: %s: %w", r.URL, err)
	}
	if f.opt.UserAgent != "" {
		req.Header.Set("User-Agent", f.opt.UserAgent)
	}

	// через собственный RoundTrip: Accept-Encoding (gzip, br — ТЗ 5.5),
	// распаковку с потолком и токен RPS получает и этот путь, и
	// http.Client поверх Fetcher (цепочка редиректов T-048) — один код
	// на оба.
	start := time.Now()
	resp, err := f.RoundTrip(req)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("httpfetch: GET %s: %w", r.URL, err)
	}

	body := io.ReadCloser(&cancelBody{rc: resp.Body, cancel: cancel})
	var limiter *BodyLimiter
	if f.lim != nil {
		// Лимиты тела T-049: обрезка по MaxBodyBytes (признак —
		// Truncated, не ошибка сети) и анти-тарпит по мин. скорости
		// после первого байта (нарушение — таймаут, соединение
		// закрывается). Отмена общего таймаута — та же cancel.
		limiter = newBodyLimiter(body, cancel, f.lim.opt)
		body = limiter
	}
	return &FetchResult{
		StatusCode:  resp.StatusCode,
		Status:      resp.Status,
		Header:      resp.Header,
		Body:        body,
		BodyLimiter: limiter,
		FinalURL:    resp.Request.URL,
		Elapsed:     time.Since(start),
		TLS:         resp.TLS,
		TLSInfo:     NewTLSInfo(resp.TLS, host),
	}, nil
}

// IsTimeout сообщает, вызвана ли ошибка Fetch таймаутом одного из
// слоёв (connect, TLS, заголовки, общий), а не отказом протокола.
// Признак нужен классификации потребителя (таймаут — temp_error, не
// «нет сайта»; ТЗ 5.5, §8) — стиль dnsclient.IsTimeout.
func IsTimeout(err error) bool {
	if err == nil {
		return false
	}
	var ne net.Error
	if errors.As(err, &ne) {
		return ne.Timeout()
	}
	return errors.Is(err, context.DeadlineExceeded)
}

// addrsKey — тип ключа ctx для адресов DNS-этапа (не строка: коллизии
// имён с внешним кодом недопустимы).
type addrsKey struct{}

// addrsValue — адреса DNS-этапа одного запроса Fetch: host —
// нормализованный хост URL (для сверки в dialContext), addrs — адреса
// подключения в порядке ответа DNS.
type addrsValue struct {
	host  string
	addrs []netip.Addr
}

// addrsFromContext извлекает адреса DNS-этапа из ctx запроса.
func addrsFromContext(ctx context.Context) *addrsValue {
	v, _ := ctx.Value(addrsKey{}).(*addrsValue)
	return v
}

// dialContext — точка подключения к заранее резолвенному IP: net/http
// передаёт сюда контекст запроса и целевой адрес «хост:порт» из URL,
// а подключаемся мы к адресам DNS-этапа или резолва редиректа.
func (f *Fetcher) dialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if network != "tcp" && network != "tcp4" && network != "tcp6" {
		return nil, fmt.Errorf("httpfetch: неподдерживаемая сеть %q", network)
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("httpfetch: адрес подключения %q: %w", addr, err)
	}
	host = strings.ToLower(host)

	// Основной хост запроса: адреса пришли из DNS-этапа вместе с
	// задачей q:http (ТЗ 5.5 п. 2) — резолва не делаем.
	if info := addrsFromContext(ctx); info != nil && info.host == host {
		return f.dialAddrs(ctx, info.addrs, port)
	}

	// Хост без адресов DNS-этапа — редирект на другой хост (ТЗ 5.5 п. 2:
	// «резолв через тот же резолвер с кэшем»). Сюда попадает и запрос
	// через RoundTrip без адресов (http.Client поверх транспорта):
	// исходный ctx не переносит адреса на новый хост — сверка выше.
	addrs, err := f.lookup(ctx, host)
	if err != nil {
		return nil, err
	}
	return f.dialAddrs(ctx, addrs, port)
}

// lookup возвращает адреса хоста: IP-литерал из URL редиректа — сам
// по себе (резолверу имя не нужно), имя — через Resolver транспорта.
func (f *Fetcher) lookup(ctx context.Context, host string) ([]netip.Addr, error) {
	if ip, err := netip.ParseAddr(host); err == nil {
		return []netip.Addr{ip}, nil
	}
	addrs, err := f.res.LookupNetIP(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("httpfetch: резолв %q: %w", host, err)
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("httpfetch: резолв %q: пустой ответ", host)
	}
	return addrs, nil
}

// dialNet — продакшн-диал (поле dial — шов подмены в тестах): TCP
// к адресу; таймаут connect и отмена приходят из ctx попытки (dialTimed).
func (f *Fetcher) dialNet(ctx context.Context, addr netip.Addr, port string) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, "tcp", net.JoinHostPort(addr.String(), port))
}

// dialTimed — одна попытка подключения с таймаутом connect (таблица
// лимитов ТЗ 5.5): поверх контекста запроса действуют и отмена, и
// общий таймаут (язык берёт ближайший дедлайн).
func (f *Fetcher) dialTimed(ctx context.Context, addr netip.Addr, port string) (net.Conn, error) {
	tctx, cancel := context.WithTimeout(ctx, f.opt.ConnectTimeout)
	defer cancel()
	return f.dial(tctx, addr, port)
}

// dialAddrs подключается к одному из адресов DNS-этапа. Стратегия при
// нескольких адресах — happy-eyeballs-подобная гонка с шагом
// FallbackDelay (RFC 8305 connection attempt delay), обобщённая с двух
// семейств адресов на весь список: первая попытка стартует сразу,
// следующая — через паузу и так далее; побеждает первое успешное
// соединение, проигравшие попытки прерываются отменой производного
// контекста, а опоздавшие соединения закрывают сами горутины-попытки.
//
// Против последовательного перебора: мёртвый адрес, молчащий на SYN,
// выжидает полный connect timeout (5 с), и домен с четырьмя A-записями,
// где жива последняя, потратил бы 20 с — весь общий бюджет запроса
// (ТЗ 5.5). Гонка ограничивает ожидание первым успешным соединением
// плюс одна пауза на каждый пропущенный адрес; «последовательный
// фолбэк» остаётся частным случаем — следующая попытка стартует
// немедленно, когда все запущенные уже упали, не дожидаясь паузы.
// Порядок попыток — порядок ответа DNS (unbound отдаёт записи зоны как
// есть; приоритет семейств — задача приоритезации адресов, не
// транспорта).
func (f *Fetcher) dialAddrs(ctx context.Context, addrs []netip.Addr, port string) (net.Conn, error) {
	switch len(addrs) {
	case 0:
		return nil, errors.New("httpfetch: нет адресов для подключения")
	case 1:
		return f.dialLimited(ctx, addrs[0], port)
	}

	// Производный контекст останавливает висящие попытки после
	// возвращения победителя (dial завершившийся соединением от
	// отмены уже не страдает — отмена действует только на идущий диал).
	stop, cancel := context.WithCancel(ctx)
	defer cancel()

	var mu sync.Mutex
	won := false
	gaveUp := false // вызывающий ушёл по отмене: победитель больше не нужен
	var winner net.Conn
	failed := 0
	var lastErr error
	event := make(chan struct{}, 1) // будит цикл ожидания; буфер 1 — сигнал не теряется

	attempt := func(i int) {
		conn, err := f.dialLimited(stop, addrs[i], port)
		mu.Lock()
		defer mu.Unlock()
		if won || gaveUp {
			if conn != nil {
				_ = conn.Close() // гонка проиграна уже после успеха
			}
			return
		}
		if conn != nil {
			won, winner = true, conn
			signal(event)
			return
		}
		failed++
		lastErr = err
		signal(event)
	}

	started := 1
	go attempt(0)
	timer := time.NewTimer(f.opt.FallbackDelay)
	defer timer.Stop()
	for {
		select {
		case <-event:
			mu.Lock()
			conn, ok, fails, err := winner, won, failed, lastErr
			mu.Unlock()
			if ok {
				return conn, nil
			}
			if fails == len(addrs) {
				return nil, fmt.Errorf("httpfetch: недоступны все %d адресов: %w", len(addrs), err)
			}
			if fails == started && started < len(addrs) {
				// все запущенные упали — следующий адрес без паузы
				go attempt(started)
				started++
				timer.Reset(f.opt.FallbackDelay)
			}
		case <-timer.C:
			if started < len(addrs) {
				go attempt(started)
				started++
				if started < len(addrs) {
					timer.Reset(f.opt.FallbackDelay)
				}
			}
		case <-ctx.Done():
			// Отмена может выиграть select уже после победы попытки:
			// установленное соединение (со слотами лимитов limitConn)
			// обязано освобождаться здесь, а не утекать с проигравшим
			// выходом. Опоздавшие попытки видит флаг gaveUp.
			mu.Lock()
			gaveUp = true
			conn := winner
			winner = nil
			mu.Unlock()
			if conn != nil {
				_ = conn.Close()
			}
			return nil, ctx.Err()
		}
	}
}

// signal отправляет неблокирующее уведомление о событии попытки.
func signal(event chan<- struct{}) {
	select {
	case event <- struct{}{}:
	default:
	}
}

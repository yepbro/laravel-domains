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
//     транспорта; обёртка-кэш — CachingResolver.
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
	// MaxBodyBytes — предел читаемого тела. Транспорт его не применяет:
	// лимит чтения (вместе с защитой от тарпитов по скорости) реализует
	// T-049; здесь значение только переносится из конфигурации к
	// потребителю (ТЗ 5.5, «макс. размер читаемого тела 2 МБ»).
	MaxBodyBytes int64
	// UserAgent — User-Agent запросов Fetch (честный, с контактами —
	// ТЗ 5.5, §9; формализация контактов — T-052). Пустой — не ставится,
	// и транспорт посылает UA Go по умолчанию.
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

// NewOptions собирает параметры из секции http конфигурации (T-006).
// FallbackDelay, Resolver и TLSClientConfig в ТЗ числами не заданы:
// пауза гонки адресов — транспортная деталь, резолвер этапа и TLS
// подключаются при сборке воркера (T-050/T-053).
func NewOptions(cfg config.HTTP) Options {
	return Options{
		ConnectTimeout:      cfg.ConnectTimeout.D(),
		TLSHandshakeTimeout: cfg.TLSHandshakeTimeout.D(),
		HeaderTimeout:       cfg.HeaderTimeout.D(),
		TotalTimeout:        cfg.TotalTimeout.D(),
		MaxBodyBytes:        cfg.MaxBodyBytes.B(),
		UserAgent:           cfg.UserAgent,
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
// Addrs обязательны: транспорт не резолвит основной хост сам — этим он
// и отличается от обычного клиента; запрос задаёт хост (Host-заголовок,
// SNI) и адреса подключения раздельно.
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
	// Лимит чтения (ТЗ 5.5 — 2 МБ) применяет потребитель (T-049);
	// допустимый предел перенесён в Options.MaxBodyBytes.
	Body io.ReadCloser
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
	f.rt = &http.Transport{
		// Прямые подключения: исходящие соединения одного IP краулера
		// (ТЗ 10.2), прокси не настраиваются.
		Proxy: nil,
		// Подключение к заранее резолвенному IP: адреса основного хоста
		// приходят в ctx запроса Fetch, хосты редиректов резолвит
		// dialContext через Resolver.
		DialContext: f.dialContext,
		// ТЗ 5.5: keep-alive «отключён или очень короткий idle». Здесь
		// отключён: запрашивается одна главная страница на сайт (минимум
		// запросов), а пул соединений по ключу «хост» переживал бы смену
		// адреса при перерезолве и держал тысячи неактивных соединений
		// при concurrency 2000. Короткий idle — на усмотрение T-049.
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
func (f *Fetcher) RoundTrip(req *http.Request) (*http.Response, error) {
	return f.rt.RoundTrip(req)
}

// Fetch выполняет GET одного URL с подключением к заранее резолвленным
// адресам (ТЗ 5.5 пп. 1–2): хост URL задаёт Host-заголовок и SNI,
// подключение идёт к Addrs. Общий таймаут (TotalTimeout, по умолчанию
// 20 с) покрывает запрос целиком, включая чтение тела вызывающим —
// до закрытия Body. Ошибочный статус ответа — не ошибка Fetch (код в
// FetchResult); error — только транспорт (сеть, таймауты, отмена ctx).
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

	start := time.Now()
	resp, err := f.rt.RoundTrip(req)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("httpfetch: GET %s: %w", r.URL, err)
	}
	return &FetchResult{
		StatusCode: resp.StatusCode,
		Status:     resp.Status,
		Header:     resp.Header,
		Body:       &cancelBody{rc: resp.Body, cancel: cancel},
		FinalURL:   resp.Request.URL,
		Elapsed:    time.Since(start),
		TLS:        resp.TLS,
		TLSInfo:    NewTLSInfo(resp.TLS, host),
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
		return f.dialTimed(ctx, addrs[0], port)
	}

	// Производный контекст останавливает висящие попытки после
	// возвращения победителя (dial завершившийся соединением от
	// отмены уже не страдает — отмена действует только на идущий диал).
	stop, cancel := context.WithCancel(ctx)
	defer cancel()

	var mu sync.Mutex
	won := false
	var winner net.Conn
	failed := 0
	var lastErr error
	event := make(chan struct{}, 1) // будит цикл ожидания; буфер 1 — сигнал не теряется

	attempt := func(i int) {
		conn, err := f.dialTimed(stop, addrs[i], port)
		mu.Lock()
		defer mu.Unlock()
		if won {
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

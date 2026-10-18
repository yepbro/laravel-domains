// Пакет dnsclient — DNS-клиент этапа проверки доменов (ТЗ 5.4): обмен
// с локальным Unbound поверх github.com/miekg/dns, собственный пул
// UDP-сокетов, TCP-fallback при усечении ответа (TC=1, RFC 1035 §4.2.1)
// и ретраи на таймаут/ошибку сети.
//
// Контракт результата принципиален для потребителя (T-036…T-039): RCODE —
// не ошибка. NXDOMAIN, SERVFAIL, REFUSED и прочие коды — легальные ответы
// сервера и возвращаются в Result.RCode без error; error — только сеть
// (таймаут, обрыв, отказ транспорта) после исчерпания попыток. ТЗ 5.4
// различает эти ветки явно: NXDOMAIN → not_delegated (RFC 8020),
// SERVFAIL/таймаут после ретраев → temp_error.
//
// Записи возвращаются сырыми ([]dns.RR секций Answer/Ns): разворачивание
// цепочек CNAME и выборка A/AAAA/CNAME/NS — задача потребителя (T-037,
// ТЗ 5.4 п. 2); RecordsOf фильтрует по типу, не интерпретируя.
//
// Точка расширения T-035 (параллелизм/QPS, адаптивное снижение, отказ
// AAAA) — интерфейс Resolver: лимитёр оборачивает Exchange декоратором,
// не меняя контракт; пул сокетов сам по себе ограничивает число
// одновременно висящих UDP-запросов размером UDPPoolSize.
package dnsclient

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/miekg/dns"

	"crawler/internal/config"
)

// Значения по умолчанию. Таймауты посчитаны от локального Unbound
// (ТЗ 5.4: рекурсивный, кэш 512–768 МБ): горячие ответы — микросекунды,
// холодные упираются в апстрим-резолвер, 2 с на UDP-попытку хватает с
// запасом; TCP-fallback реже и дороже (установка соединения) — 5 с.
const (
	// DefaultTimeout — таймаут одной UDP-попытки (запрос + ожидание
	// ответа, включая ожидание сокета из пула).
	DefaultTimeout = 2 * time.Second
	// DefaultTCPTimeout — таймаут TCP-fallback: установка соединения,
	// обмен, чтение усечённого ответа целиком.
	DefaultTCPTimeout = 5 * time.Second
	// DefaultRetryDelay — пауза между попытками: даёт резолверу
	// разгрести очередь/восстановиться, не разбирая ошибку немедленно.
	DefaultRetryDelay = 200 * time.Millisecond

	// DefaultEDNS0UDPSize — advertised размер UDP-буфера в OPT RR
	// (RFC 6891, EDNS0): 1232 байта — безопасный размер DNS flag day
	// 2020 (влезает в транзитный MTU 1280 без фрагментации). Без OPT
	// резолвер обязан укладываться в исторические 512 байт и режет
	// всё крупнее до TC=1 — такой ответ в каждой попытке уходил бы в
	// дорогой TCP-fallback (DNS-07).
	DefaultEDNS0UDPSize = 1232
)

// DefaultRetries — повторы сверх первой попытки (итого до 3 обменов).
// ТЗ число не задаёт; 3 попытки — политика повторов системы (доставка
// очередей T-025, перепроверки temp_error T-031). Ограничение попыток
// одного запроса и повторные эпизоды проверки (T-031) — разные уровни:
// здесь гасим секундные сбои, там — устойчивые проблемы резолвера.
const DefaultRetries = 2

// MaxRetries — верхняя граница ретраев: 10 попыток × 2 с = 20 с на
// мёртвом резолвере; больше — работа для планировщика (T-031), а не
// для одного запроса.
const MaxRetries = 10

// DefaultUDPPoolSize — размер пула UDP-сокетов по умолчанию. Равен
// dns.concurrency из ТЗ 5.10 (2000): пул обязан вмещать плановый
// параллелизм, иначе сам становится ограничителем ниже настроенного
// (ждущие Get съедают таймаут попытки). 2000 файловых дескрипторов на
// воркер — обычная величина для Linux (no_file в Docker ≥ 1M).
const DefaultUDPPoolSize = 2000

// Question — один DNS-вопрос (ТЗ 5.4 пп. 1–3: A, AAAA, CNAME, NS).
type Question struct {
	// Name — полное имя домена (апекс или субдомен) в punycode.
	// Нормализация имени — зона ответственности потребителя (internal/idn,
	// ТЗ 5.2); здесь имя только дополняется завершающей точкой.
	Name string
	// Type — тип записи (dns.TypeA, dns.TypeAAAA, dns.TypeCNAME,
	// dns.TypeNS); один вопрос — один тип, смешанные запросы DNS
	// не использует.
	Type uint16
}

// validate проверяет вопрос до любых сетевых попыток: ошибка упаковки
// детерминирована, гонять её по ретраям бессмысленно.
func (q Question) validate() error {
	if strings.TrimSpace(q.Name) == "" {
		return errors.New("dnsclient: имя вопроса пусто")
	}
	// Признак рабочего типа — наличие RR-реализации в miekg/dns: у
	// псевдотипов из TypeToString (None, Reserved) записей нет, как и у
	// сервисных (AXFR/IXFR); заодно гарантируется разбор ответа.
	if dns.TypeToRR[q.Type] == nil {
		return fmt.Errorf("dnsclient: неизвестный тип записи %d (ожидаются dns.TypeA/AAAA/CNAME/NS/…)", q.Type)
	}
	return nil
}

// RCode — код ответа сервера (RFC 1035 §4.1.1; расширенные коды —
// числом). Часть Result, не ошибка: см. комментарий пакета.
type RCode int

// Стандартные коды (младшие 4 бита заголовка). Значения совпадают с
// константами dns.Rcode* miekg/dns.
const (
	RCodeNOERROR  RCode = dns.RcodeSuccess
	RCodeFORMERR  RCode = dns.RcodeFormatError
	RCodeSERVFAIL RCode = dns.RcodeServerFailure
	RCodeNXDOMAIN RCode = dns.RcodeNameError
	RCodeNOTIMP   RCode = dns.RcodeNotImplemented
	RCodeREFUSED  RCode = dns.RcodeRefused
)

// String возвращает мнемонику кода (NOERROR, NXDOMAIN, …) либо RCODEn.
func (r RCode) String() string {
	if s, ok := dns.RcodeToString[int(r)]; ok {
		return s
	}
	return fmt.Sprintf("RCODE%d", int(r))
}

// Result — итог одного Exchange: код ответа, сырые записи и метрики
// обмена. Поля Answer/Ns переносятся из ответа как есть (владение
// передаётся вызывающему).
type Result struct {
	// RCode — код ответа сервера (NOERROR у пустого ответа — NODATA,
	// легальный итог «имя есть, записей этого типа нет»).
	RCode RCode
	// Answer — секция answer: A/AAAA/CNAME/NS вперемешку с прочими
	// (например, CNAME-цепочка тянется перед целевым A). Сырые записи —
	// интерпретация (цепочки CNAME, wildcard-сравнение T-038) за
	// потребителем.
	Answer []dns.RR
	// Ns — секция authority (SOA при NXDOMAIN, NS делегирования при
	// рефералах) — нужна потребителю T-036 («NS при необходимости»).
	Ns []dns.RR
	// Elapsed — время Exchange целиком: все попытки и паузы между ними.
	Elapsed time.Duration
	// Attempts — число сетевых обменов (каждая UDP-попытка и каждый
	// TCP-fallback считаются отдельно).
	Attempts int
	// TCP — итоговый ответ получен по TCP (сработал fallback по TC=1).
	TCP bool
}

// RecordsOf возвращает записи секции answer указанного типа
// (dns.TypeA, dns.TypeAAAA, …) — фильтр без интерпретации.
func (r *Result) RecordsOf(t uint16) []dns.RR {
	var out []dns.RR
	for _, rr := range r.Answer {
		if rr.Header().Rrtype == t {
			out = append(out, rr)
		}
	}
	return out
}

// Resolver — точка интеграции DNS-этапа: потребитель (dns-worker,
// T-036…T-039) зависит от интерфейса, а не от реализации. T-035
// оборачивает его декоратором с лимитами параллелизма/QPS и фильтром
// AAAA (dns.ipv6), не меняя контракт.
type Resolver interface {
	// Exchange выполняет вопрос к резолверу. Ошибка — только сеть
	// (после исчерпания ретраев) или отмена ctx; RCODE — в Result.
	Exchange(ctx context.Context, q Question) (*Result, error)
}

// compile-time check: *Client — Resolver.
var _ Resolver = (*Client)(nil)

// Options — параметры клиента. Нулевые значения заменяются дефолтами
// (withDefaults); Addr обязателен.
type Options struct {
	// Addr — адрес резолвера host:port (первый из dns.resolvers —
	// локальный Unbound, ТЗ 5.4). Один клиент — один резолвер:
	// публичные fallback-резолверы (dns.fallback_resolvers) — цепочка
	// FallbackResolver выше транспорта (fallback.go, T-107).
	Addr string
	// Timeout — таймаут одной UDP-попытки. По умолчанию 2s.
	Timeout time.Duration
	// TCPTimeout — таймаут TCP-fallback (соединение + обмен).
	// По умолчанию 5s.
	TCPTimeout time.Duration
	// Retries — повторы сверх первой попытки при таймауте/ошибке сети.
	// По умолчанию 2 (итого до 3 обменов); 0/отрицательное — тоже
	// дефолт: режима «без ретраев» нет (ТЗ 5.4 ретраи требует).
	Retries int
	// RetryDelay — пауза между попытками. По умолчанию 200ms.
	RetryDelay time.Duration
	// UDPPoolSize — размер пула UDP-сокетов (см. DefaultUDPPoolSize).
	UDPPoolSize int
}

// NewOptions собирает параметры из секции dns конфигурации (T-006):
// адрес — первый резолвер (клиент работает с одним сервером — Unbound);
// таймауты/ретраи/пул — собственные поля секции.
func NewOptions(cfg config.DNS) Options {
	addr := ""
	if len(cfg.Resolvers) > 0 {
		addr = cfg.Resolvers[0]
	}
	return Options{
		Addr:        addr,
		Timeout:     cfg.Timeout.D(),
		TCPTimeout:  cfg.TCPTimeout.D(),
		Retries:     cfg.Retries,
		RetryDelay:  cfg.RetryDelay.D(),
		UDPPoolSize: cfg.UDPPoolSize,
	}
}

// withDefaults заменяет нулевые и отрицательные значения дефолтами
// (репо-стиль db.Options: «0 — значение по умолчанию»). Отдельного
// режима «без ретраев» нет: ТЗ 5.4 ретраи требует, минимум — один
// повтор; «без паузы» аналогично не нужен.
func (o Options) withDefaults() Options {
	if o.Timeout <= 0 {
		o.Timeout = DefaultTimeout
	}
	if o.TCPTimeout <= 0 {
		o.TCPTimeout = DefaultTCPTimeout
	}
	if o.Retries <= 0 {
		o.Retries = DefaultRetries
	}
	if o.RetryDelay <= 0 {
		o.RetryDelay = DefaultRetryDelay
	}
	if o.UDPPoolSize <= 0 {
		o.UDPPoolSize = DefaultUDPPoolSize
	}
	return o
}

// validate проверяет значения после подстановки дефолтов. Проверки
// таймаутов/пула избыточны (withDefaults уже нормализовала) и здесь не
// повторяются; ловится то, что нормализация не исправляет осмысленно.
func (o Options) validate() error {
	if _, _, err := net.SplitHostPort(o.Addr); err != nil {
		return fmt.Errorf("dnsclient: адрес резолвера: %w", err)
	}
	if o.Retries > MaxRetries {
		return fmt.Errorf("dnsclient: retries %d: максимум %d", o.Retries, MaxRetries)
	}
	// Защита от прямого конструирования мимо NewClient.
	if o.UDPPoolSize < 1 {
		return fmt.Errorf("dnsclient: udp_pool_size %d: ожидается ≥ 1", o.UDPPoolSize)
	}
	return nil
}

// exchangeFunc — транспорт одного сетевого обмена: отправить сообщение,
// прочитать ответ (tcp — TCP-fallback по TC=1). Отдельный тип — шов
// подмены транспорта в юнит-тестах без сети; продакшн-реализация —
// exchangeNet: пул UDP-сокетов и TCP-соединение на запрос.
type exchangeFunc func(ctx context.Context, m *dns.Msg, tcp bool) (*dns.Msg, error)

// Client — DNS-клиент над одним резолвером. Безопасен для одновременного
// использования из горутин (пул сокетов — единственное разделяемое
// состояние); лимит одновременных Exchange — размер пула: ждущие
// организуются в очередь Get.
type Client struct {
	opt      Options
	pool     *UDPPool
	bufs     sync.Pool // *[]byte: UDP-буферы чтения (64 КиБ) между попытками
	exchange exchangeFunc
}

// NewClient создаёт клиента и заранее открывает пул UDP-сокетов.
// fail-fast старта ограничен самим адресом: ошибка набора (кривой адрес,
// порт, маршрут) — ошибка создания пула на старте; доступность самого
// резолвера UDP connect не проверяет (пакетов не шлёт) — мёртвый сервер
// проявляется в первом же обмене (ECONNREFUSED/таймаут → temp_error)
// и виден в метриках лимитёра (crawler_dns_limit_timeout_ratio, DNS-06),
// а не ошибкой старта. Стартовая синтетическая проба резолвера и
// переключение на dns.fallback_resolvers — декораторы выше клиента
// (probe.go, fallback.go, T-107).
// Контекст ограничивает открытие сокетов.
func NewClient(ctx context.Context, opts Options) (*Client, error) {
	opts = opts.withDefaults()
	if err := opts.validate(); err != nil {
		return nil, err
	}
	pool, err := NewUDPPool(ctx, opts.Addr, opts.UDPPoolSize)
	if err != nil {
		return nil, err
	}
	c := &Client{opt: opts, pool: pool}
	c.bufs.New = func() any {
		b := make([]byte, dns.MaxMsgSize)
		return &b
	}
	c.exchange = c.exchangeNet
	return c, nil
}

// Close закрывает пул сокетов; клиент после этого непригоден.
func (c *Client) Close() error { return c.pool.Close() }

// Exchange выполняет вопрос к резолверу с ретраями (ТЗ 5.4).
//
// Попытка = UDP-обмен; ответ с TC=1 повторяется по TCP (свой таймаут)
// тем же вопросом — TCP-ошибка делает попытку неудачной целиком, цикл
// начинается заново с UDP. Ошибки сети — до Retries повторов с паузой
// RetryDelay; отмена ctx проверяется до попытки, после каждой неудачи и
// в паузе (между попытками Exchange прерывается сразу).
func (c *Client) Exchange(ctx context.Context, q Question) (*Result, error) {
	if err := q.validate(); err != nil {
		return nil, err
	}
	msg := new(dns.Msg)
	msg.SetQuestion(dns.Fqdn(q.Name), q.Type) // Id и RD выставляет SetQuestion
	// EDNS0 (RFC 6891): OPT RR с advertised размером DefaultEDNS0UDPSize —
	// резолвер отвечает до 1232 байт по UDP, длинные CNAME-цепочки и
	// большие RRset не уходят на TC=1/TCP-fallback в каждой попытке.
	// DO не выставляется: DNSSEC не запрашивается (ТЗ 5.4 не требует);
	// OPT придёт в Additional ответа и на разбор Answer/Ns не влияет.
	msg.SetEdns0(DefaultEDNS0UDPSize, false)
	if _, err := msg.Pack(); err != nil {
		// Ошибка упаковки детерминирована (длина/символы имени) —
		// ретраи не изменят исход.
		return nil, fmt.Errorf("dnsclient: некорректное имя вопроса %q: %w", q.Name, err)
	}

	start := time.Now()
	attempts := 0
	var lastErr error
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		resp, n, viaTCP, err := c.attempt(ctx, msg)
		attempts += n
		if err == nil {
			return &Result{
				RCode:    RCode(resp.Rcode),
				Answer:   resp.Answer,
				Ns:       resp.Ns,
				Elapsed:  time.Since(start),
				Attempts: attempts,
				TCP:      viaTCP,
			}, nil
		}
		lastErr = err
		if ctxErr := ctx.Err(); ctxErr != nil {
			// Отмена/дедлайн самого вызова — не сетевая неудача,
			// ретраев не полагается.
			return nil, ctxErr
		}
		if attempt >= c.opt.Retries {
			break
		}
		if c.opt.RetryDelay > 0 {
			timer := time.NewTimer(c.opt.RetryDelay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
		}
	}
	return nil, fmt.Errorf("dnsclient: %s %s: исчерпаны попытки (%d сетевых обменов): %w",
		qTypeName(q.Type), q.Name, attempts, lastErr)
}

// qTypeName возвращает мнемонику типа записи (A, AAAA, …).
func qTypeName(t uint16) string {
	if s, ok := dns.TypeToString[t]; ok {
		return s
	}
	return fmt.Sprintf("TYPE%d", t)
}

// attempt — одна попытка цикла ретраев: UDP-обмен и, при усечении
// ответа (TC=1), повтор того же вопроса по TCP. Возвращает ответ,
// число сетевых обменов (1 или 2), признак «итог по TCP» и ошибку.
func (c *Client) attempt(ctx context.Context, msg *dns.Msg) (*dns.Msg, int, bool, error) {
	actx, cancel := context.WithTimeout(ctx, c.opt.Timeout)
	defer cancel()

	// Копия с новым Id на каждый обмен: у сокета один запрос в полёте,
	// но сокеты возвращаются в пул, и поздний ответ прежнего обмена
	// может прийти в начале следующего — свежий Id отбрасывает его
	// на проверке в udpExchange.
	m := *msg
	m.Id = dns.Id()
	resp, err := c.exchange(actx, &m, false)
	if err != nil {
		return nil, 1, false, fmt.Errorf("udp: %w", err)
	}
	if !resp.Truncated {
		return resp, 1, false, nil
	}

	// TC=1: сервер усёк ответ (UDP-датаграмма мала) — тот же вопрос
	// по TCP, где ограничения на размер датаграммы нет (RFC 1035
	// §4.2.2). Таймаут свой: TCP-fallback дороже (установка соединения,
	// передача полного ответа).
	tctx, tcancel := context.WithTimeout(ctx, c.opt.TCPTimeout)
	defer tcancel()
	m.Id = dns.Id()
	tresp, err := c.exchange(tctx, &m, true)
	if err != nil {
		return nil, 2, false, fmt.Errorf("tcp-fallback: %w", err)
	}
	return tresp, 2, true, nil
}

// exchangeNet — сетевой транспорт: UDP через пул сокетов, TCP —
// соединение на запрос.
func (c *Client) exchangeNet(ctx context.Context, m *dns.Msg, tcp bool) (*dns.Msg, error) {
	if tcp {
		return c.tcpExchange(ctx, m)
	}
	return c.udpExchange(ctx, m)
}

// udpExchange выполняет обмен через пул подключённых UDP-сокетов.
// Дедлайн попытки берётся из ctx (таймаут уже учтён в attempt).
//
// Ответы с чужим Id и не разбираемые датаграммы пропускаются: сокет
// переиспользуется, и после таймаута на нём может лежать поздний ответ
// прежнего запроса; чтение продолжается до дедлайна собственного.
func (c *Client) udpExchange(ctx context.Context, m *dns.Msg) (*dns.Msg, error) {
	conn, err := c.pool.Get(ctx)
	if err != nil {
		return nil, fmt.Errorf("сокет из пула: %w", err)
	}
	defer c.pool.Put(conn)

	wire, err := m.Pack()
	if err != nil {
		return nil, fmt.Errorf("упаковка запроса: %w", err)
	}
	dl, ok := ctx.Deadline()
	if !ok {
		dl = time.Now().Add(c.opt.Timeout) // страховка: транспорт обязан быть ограничен по времени
	}
	_ = conn.SetDeadline(dl)
	if _, err := conn.Write(wire); err != nil {
		return nil, err
	}

	bufp := c.bufs.Get().(*[]byte)
	defer c.bufs.Put(bufp)
	buf := *bufp
	for {
		n, err := conn.Read(buf)
		if err != nil {
			return nil, err
		}
		resp := new(dns.Msg)
		if err := resp.Unpack(buf[:n]); err != nil {
			continue // битый датаграмм — ждём свой ответ до дедлайна
		}
		if resp.Id != m.Id {
			continue // поздний ответ прежнего обмена на этом сокете
		}
		return resp, nil
	}
}

// tcpExchange — TCP-fallback: соединение на запрос, без пула.
//
// Обоснование: TC=1 — редкое событие (обычные ответы A/AAAA/CNAME/NS
// помещаются в 512-байтовый датаграмм), держать постоянный пул
// TCP-соединений ради редкой ветки — постоянные издержки (keepalive,
// переустановка после idle-таймаута резолвера) без выигрыша.
// Стоимость установки соединения к локальному Unbound — доли
// миллисекунды; свежее соединение к тому же не таит состояния,
// оставшегося от чужих запросов.
func (c *Client) tcpExchange(ctx context.Context, m *dns.Msg) (*dns.Msg, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", c.opt.Addr)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	}
	dconn := &dns.Conn{Conn: conn}
	if err := dconn.WriteMsg(m); err != nil {
		return nil, err
	}
	resp, err := dconn.ReadMsg()
	if err != nil {
		return nil, err
	}
	if resp.Id != m.Id {
		return nil, fmt.Errorf("идентификатор ответа %d не совпадает с запросом %d", resp.Id, m.Id)
	}
	return resp, nil
}

// IsTimeout сообщает, вызвана ли ошибка Exchange таймаутом (дедлайн
// попытки, ожидание сокета из пула), а не отказом сети/протокола.
// Различение нужно адаптивному снижению нагрузки T-035 (ТЗ 5.4: доля
// таймаутов) и диагностике потребителя (T-036: SERVFAIL/таймаут).
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

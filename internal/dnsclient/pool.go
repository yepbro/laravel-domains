// Пул подключённых UDP-сокетов к одному резолверу (ТЗ 5.4: «собственный
// пул UDP-сокетов»).
//
// Почему свой пул, а не dns.Client miekg/dns: Exchange у dns.Client
// открывает сокет на каждый запрос — при плановом параллелизме 2000
// (ТЗ 5.10 dns.concurrency) это 2000 операций socket(2)+connect(2) в
// секунду на каждый тысячный QPS и такой же поток закрытий; заранее
// подключённые (connect(2), без трафика — UDP connect лишь фиксирует
// адрес партнёра) сокеты убирают обе. Подключённость даёт и фильтрацию
// ядром датаграмм от чужих источников, и ошибку ECONNREFUSED от
// недоступного резолвера (ICMP port unreachable) вместо молчания.
//
// Один сокет — один запрос в полёте: Exchange держит сокет от Get до
// Put, поэтому размер пула ограничивает и число одновременных
// UDP-обменов (ждущие выстраиваются в Get; это же — естественная
// граница под лимитёр T-035). Ожидающим сокетам не грозит перехват
// чужих ответов: следующий владелец сокета работает с собственным Id
// (см. Client.attempt), поздние ответы прежних обменов отбрасываются.

package dnsclient

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
)

// ErrPoolClosed возвращают Get после Close (Put молча закрывает сокет:
// обмен с ним уже завершён, ждать некого и нечего).
var ErrPoolClosed = errors.New("dnsclient: пул UDP-сокетов закрыт")

// UDPPool — пул подключённых UDP-сокетов одного адреса. Безопасен для
// одновременного использования из горутин.
type UDPPool struct {
	addr string
	size int
	free chan *net.UDPConn
	// done закрывается в Close: ждущие Get просыпаются с ErrPoolClosed,
	// не дожидаясь дедлайна собственного ctx (DNS-09).
	done chan struct{}

	mu     sync.Mutex
	live   int // открытые сокеты: свободные + выданные + набираемые
	closed bool

	// dial открывает один сокет; подменяется тестами (транспортный шов).
	dial func(ctx context.Context) (*net.UDPConn, error)
}

// NewUDPPool создаёт пул и заранее открывает все size сокетов: ошибка
// набора (кривой адрес/порт, нет маршрута) — ошибка создания пула на
// старте, а не первого запроса. Доступность резолвера connect не
// проверяет (UDP connect пакетов не шлёт): мёртвый сервер звучит в
// первом обмене — ECONNREFUSED от подключённого сокета (ICMP port
// unreachable) либо таймаут; см. NewClient о стартовой пробе (T-107).
// Контекст ограничивает открытие.
func NewUDPPool(ctx context.Context, addr string, size int) (*UDPPool, error) {
	if size < 1 {
		return nil, fmt.Errorf("dnsclient: размер пула UDP-сокетов %d: ожидается ≥ 1", size)
	}
	if _, _, err := net.SplitHostPort(addr); err != nil {
		return nil, fmt.Errorf("dnsclient: адрес резолвера %q: ожидается host:port: %w", addr, err)
	}
	p := &UDPPool{
		addr: addr,
		size: size,
		free: make(chan *net.UDPConn, size),
		done: make(chan struct{}),
	}
	p.dial = p.dialUDP
	for i := 0; i < size; i++ {
		conn, err := p.dial(ctx)
		if err != nil {
			_ = p.Close()
			return nil, fmt.Errorf("dnsclient: открытие пула UDP-сокетов к %s (сокет %d из %d): %w", addr, i+1, size, err)
		}
		p.free <- conn
	}
	p.mu.Lock()
	p.live = size
	p.mu.Unlock()
	return p, nil
}

// dialUDP подключает один сокет к адресу пула.
func (p *UDPPool) dialUDP(ctx context.Context) (*net.UDPConn, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "udp", p.addr)
	if err != nil {
		return nil, err
	}
	udp, ok := conn.(*net.UDPConn)
	if !ok {
		_ = conn.Close()
		return nil, fmt.Errorf("dnsclient: %s: соединение не UDP (%T)", p.addr, conn)
	}
	return udp, nil
}

// Get выдаёт свободный сокет, при нехватке — открывает новый (не выше
// размера пула), иначе ждёт освобождения или отмены ctx. Сокет
// возвращается Put или списывается Discard; на время владения
// обязательна своя пара Get/Put на обмен.
func (p *UDPPool) Get(ctx context.Context) (*net.UDPConn, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrPoolClosed
	}
	if p.live < p.size {
		// Место есть (после Discard или неудачного набора), но счётчик
		// live в этих состояниях не обязан отражать фактическое число
		// живых сокетов (инвариант live == свободные + выданные не
		// восстанавливается) — сначала свободный сокет из канала, и
		// только при пустом канале набор нового: иначе Get открывал бы
		// лишний сокет рядом с лежащим свободным (DNS-08).
		select {
		case conn := <-p.free:
			p.mu.Unlock()
			return conn, nil
		default:
		}
		// Набор нового сокета: счётчик занимает место заранее, чтобы
		// параллельные Get не перебрали размер пула.
		p.live++
		p.mu.Unlock()
		conn, err := p.dial(ctx)
		if err != nil {
			p.mu.Lock()
			p.live--
			p.mu.Unlock()
			return nil, err
		}
		return conn, nil
	}
	p.mu.Unlock()

	select {
	case conn := <-p.free:
		return conn, nil
	case <-p.done:
		return nil, ErrPoolClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Put возвращает сокет в пул. Отправка в канал — под мьютексом Close:
// иначе между проверкой closed и отправкой Close успевает слить канал,
// и сокет навсегда зависал бы в канале (утечка дескриптора). Закрытый
// пул принимает сокет, закрывая его: обмен с ним завершён, ждать
// освобождения больше некому.
func (p *UDPPool) Put(conn *net.UDPConn) {
	if conn == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		_ = conn.Close()
		return
	}
	select {
	case p.free <- conn:
	default:
		// Не должно случаться (выдаётся не больше size сокетов) —
		// страховка от утечки дескриптора.
		_ = conn.Close()
	}
}

// Discard списывает сокет (закрывает и исключает из пула): для случаев,
// когда сокет заведомо испорчен. Замещающий сокет откроется лениво
// ближайшим Get — отказ набора не блокирует пул навсегда.
func (p *UDPPool) Discard(conn *net.UDPConn) {
	if conn == nil {
		return
	}
	_ = conn.Close()
	p.mu.Lock()
	p.live--
	p.mu.Unlock()
}

// Close закрывает свободные сокеты и будит ждущих Get (done →
// ErrPoolClosed — без этого ждущий освобождался только дедлайном
// собственного ctx, а с context.Background висел бы вечно); выданные
// сокеты закроются при их возврате (Put в закрытый пул закрывает сокет).
// Повторный Close — no-op.
func (p *UDPPool) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.mu.Unlock()
	close(p.done) // будит ждущих Get — сокетов в закрытом пуле больше нет
	for {
		select {
		case conn := <-p.free:
			_ = conn.Close()
			p.mu.Lock()
			p.live--
			p.mu.Unlock()
		default:
			return nil
		}
	}
}

// Тесты пула UDP-сокетов. Сокеты — настоящие, на петле 127.0.0.1
// (копятся в буфере приёмника, без внешнего стека); сетевого обмена
// нет: проверяется только выдача/возврат/параллельность.

package dnsclient

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// loopUDPAddr открывает свободный UDP-адрес на петле: подключённым
// сокетам пула нужен валидный партнёр, чтобы отправка не порождала
// ICMP и ошибок — пул тестируется без обмена.
func loopUDPAddr(t *testing.T) string {
	t.Helper()
	c, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen udp: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c.LocalAddr().String()
}

// assertConnClosed проверяет, что сокет закрыт (чтение возвращает ошибку
// «использовано закрытое соединение», а не таймаут).
func assertConnClosed(t *testing.T, conn *net.UDPConn) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := conn.Read(make([]byte, 16)); err == nil {
		t.Fatal("ожидается ошибка чтения закрытого сокета, получен ответ")
	}
}

func TestUDPPoolReuseSocket(t *testing.T) {
	addr := loopUDPAddr(t)
	p, err := NewUDPPool(context.Background(), addr, 1)
	if err != nil {
		t.Fatalf("NewUDPPool: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	// Пул одного сокета: возвращённый сокет обязан выдаваться снова,
	// новых Dial не появляется (канал free — FIFO, переиспользование
	// проверяется на размере 1, где сокет единственный).
	c1, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	p.Put(c1)
	c2, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if c1 != c2 {
		t.Fatal("возвращённый сокет должен переиспользоваться, получен новый")
	}
	p.Put(c2)
}

func TestUDPPoolGetWaitsForFreeSocket(t *testing.T) {
	addr := loopUDPAddr(t)
	p, err := NewUDPPool(context.Background(), addr, 1)
	if err != nil {
		t.Fatalf("NewUDPPool: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	c1, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	// Пул исчерпан: ждущий Get укладывается в свой таймаут ожидания.
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	if c, err := p.Get(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ожидается DeadlineExceeded, получены %v, %v", c, err)
	}

	// Освобождение будит ждущего.
	got := make(chan *net.UDPConn, 1)
	errCh := make(chan error, 1)
	go func() {
		c, err := p.Get(context.Background())
		if err != nil {
			errCh <- err
			return
		}
		got <- c
	}()
	// Дать ждущему время встать в select, затем вернуть сокет.
	time.Sleep(30 * time.Millisecond)
	p.Put(c1)
	select {
	case c := <-got:
		if c != c1 {
			t.Fatal("ждущий должен получить освободившийся сокет")
		}
		p.Put(c)
	case err := <-errCh:
		t.Fatalf("get после освобождения: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("Get не дождался освобождения сокета")
	}
}

func TestUDPPoolGetCancelledWhileWaiting(t *testing.T) {
	addr := loopUDPAddr(t)
	p, err := NewUDPPool(context.Background(), addr, 1)
	if err != nil {
		t.Fatalf("NewUDPPool: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	c1, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer p.Put(c1)

	ctx, cancel := context.WithCancel(context.Background())
	res := make(chan error, 1)
	go func() {
		_, err := p.Get(ctx)
		res <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-res:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ожидается context.Canceled, получено %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Get не отреагировал на отмену контекста")
	}
}

// TestUDPPoolParallelInvariants — конкурентная работа (гонки ловит
// -race): сокет никогда не выдаётся двум владельцам одновременно,
// всего сокетов не больше размера пула.
func TestUDPPoolParallelInvariants(t *testing.T) {
	addr := loopUDPAddr(t)
	const size = 4
	p, err := NewUDPPool(context.Background(), addr, size)
	if err != nil {
		t.Fatalf("NewUDPPool: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	var (
		mu      sync.Mutex
		inUse   = make(map[*net.UDPConn]bool)
		seenAll = make(map[*net.UDPConn]bool)
	)
	const workers = 16
	const iters = 40
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			for i := 0; i < iters; i++ {
				conn, err := p.Get(ctx)
				if err != nil {
					t.Errorf("get: %v", err)
					return
				}
				mu.Lock()
				if inUse[conn] {
					mu.Unlock()
					t.Errorf("сокет %v выдан повторно при живом владельце", conn)
					p.Put(conn)
					return
				}
				inUse[conn] = true
				seenAll[conn] = true
				mu.Unlock()

				time.Sleep(time.Millisecond) // «работа» с сокетом

				mu.Lock()
				delete(inUse, conn)
				mu.Unlock()
				p.Put(conn)
			}
		}()
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(inUse) != 0 {
		t.Fatalf("после завершения всех горутин сокеты числятся выданными: %d", len(inUse))
	}
	if len(seenAll) == 0 || len(seenAll) > size {
		t.Fatalf("число различных сокетов пула %d вне 1..%d", len(seenAll), size)
	}
}

func TestUDPPoolDiscardReplacedLazily(t *testing.T) {
	addr := loopUDPAddr(t)
	p, err := NewUDPPool(context.Background(), addr, 2)
	if err != nil {
		t.Fatalf("NewUDPPool: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	// Считаем наборы после стартового eager-набора (DNS-08): рядом со
	// свободным сокетом Get не открывает новый.
	var dials atomic.Int32
	old := p.dial
	p.dial = func(ctx context.Context) (*net.UDPConn, error) {
		dials.Add(1)
		return old(ctx)
	}

	c1, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	p.Discard(c1)
	assertConnClosed(t, c1)

	// Место освободилось, но в канале лежит свободный eager-сокет —
	// Get отдаёт его, а не набирает замещающий.
	c2, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("get после discard: %v", err)
	}
	if c2 == c1 {
		t.Fatal("списанный сокет не выдаётся повторно")
	}
	if n := dials.Load(); n != 0 {
		t.Fatalf("наборов %d рядом со свободным сокетом, want 0", n)
	}

	// Канал пуст, место есть — теперь Get лениво набирает замещающий.
	c3, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if c3 == c1 || c3 == c2 {
		t.Fatal("ожидался замещающий (новый) сокет")
	}
	if n := dials.Load(); n != 1 {
		t.Fatalf("наборов %d, want 1 (ленивый замещающий)", n)
	}
	p.Put(c2)
	p.Put(c3)
}

func TestUDPPoolClose(t *testing.T) {
	addr := loopUDPAddr(t)
	p, err := NewUDPPool(context.Background(), addr, 2)
	if err != nil {
		t.Fatalf("NewUDPPool: %v", err)
	}

	// Один сокет выдан, второй свободен.
	held, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	assertConnClosed(t, held) // выданный закрывается при возврате…
	p.Put(held)               // …и не возвращается в закрытый пул
	if _, err := p.Get(context.Background()); !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("get после close: ожидается ErrPoolClosed, получено %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("повторный close должен быть no-op: %v", err)
	}
}

// TestUDPPoolCloseWakesWaiters — Close будит ждущих Get (done-канал):
// без дедлайна собственного ctx ждущий получает ErrPoolClosed, а не
// висит до конца процесса (DNS-09).
func TestUDPPoolCloseWakesWaiters(t *testing.T) {
	addr := loopUDPAddr(t)
	p, err := NewUDPPool(context.Background(), addr, 1)
	if err != nil {
		t.Fatalf("NewUDPPool: %v", err)
	}

	held, err := p.Get(context.Background()) // единственный сокет занят
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	res := make(chan error, 1)
	go func() {
		_, err := p.Get(context.Background()) // ctx без дедлайна
		res <- err
	}()
	time.Sleep(30 * time.Millisecond) // ждущий встал в select
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case err := <-res:
		if !errors.Is(err, ErrPoolClosed) {
			t.Fatalf("ждущий Get после Close: %v, want ErrPoolClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close не разбудил ждущего Get")
	}
	p.Put(held)
}

func TestUDPPoolBadArguments(t *testing.T) {
	if _, err := NewUDPPool(context.Background(), "127.0.0.1:53", 0); err == nil {
		t.Fatal("size 0: ожидается ошибка")
	}
	if _, err := NewUDPPool(context.Background(), "nohostport", 1); err == nil {
		t.Fatal("кривой адрес: ожидается ошибка")
	}
	// Порт вне диапазона: разберётся SplitHostPort, но не DialContext —
	// eager-набор обязан упасть, а не отдать мёртвый пул.
	if _, err := NewUDPPool(context.Background(), "127.0.0.1:99999", 1); err == nil {
		t.Fatal("недоступный порт: ожидается ошибка eager-набора")
	}
}

// TestUDPPoolDialFailureAtGet — ленивый набор после Discard ошибкой не
// ломает счётчик: следующая попытка снова набирает сокет.
func TestUDPPoolDialFailureAtGet(t *testing.T) {
	addr := loopUDPAddr(t)
	p, err := NewUDPPool(context.Background(), addr, 1)
	if err != nil {
		t.Fatalf("NewUDPPool: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	c1, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	p.Discard(c1)

	var attempts atomic.Int32
	old := p.dial
	p.dial = func(ctx context.Context) (*net.UDPConn, error) {
		if attempts.Add(1) == 1 {
			return nil, errors.New("dial boom")
		}
		return old(ctx)
	}
	if _, err := p.Get(context.Background()); err == nil {
		t.Fatal("первый (падающий) набор: ожидается ошибка")
	}
	// Счётчик места освобождён: повторный Get снова набирает сокет.
	c2, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("get после неудачного набора: %v", err)
	}
	p.Put(c2)
}

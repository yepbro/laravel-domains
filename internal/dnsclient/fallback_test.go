// Тесты переключения на резервные резолверы (T-107). Сети нет: элементы
// цепи — фейки Resolver с записью порядка вызовов и программируемым
// итогом. Проверяются: условие переключения (только транспортный отказ,
// RCODE — нет), порядок цепи, счётчики Stats, агрегированная ошибка
// исчерпания, отмена ctx и конкурентность (гонки ловит -race).

package dnsclient

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/miekg/dns"
)

// --- вспомогательные фейки ---------------------------------------------------

// fbCalls — разделяемая запись порядка вызовов цепи (потокобезопасна:
// конкурентный тест гоняет Exchange из горутин).
type fbCalls struct {
	mu    sync.Mutex
	order []string
}

func (c *fbCalls) add(name string) {
	c.mu.Lock()
	c.order = append(c.order, name)
	c.mu.Unlock()
}

func (c *fbCalls) snapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.order...)
}

// fbFake — элемент цепи: считает вызовы, возвращает заданный итог;
// fn (если задана) полностью заменяет поведение (отмена ctx, паузы).
type fbFake struct {
	name  string
	calls *fbCalls
	res   *Result
	err   error
	fn    func(ctx context.Context, q Question) (*Result, error)
}

func (f *fbFake) Exchange(ctx context.Context, q Question) (*Result, error) {
	if f.calls != nil { // фейки вне цепи (пробы) не регистрируются
		f.calls.add(f.name)
	}
	if f.fn != nil {
		return f.fn(ctx, q)
	}
	if f.err != nil {
		return nil, f.err
	}
	if f.res != nil {
		return f.res, nil
	}
	return &Result{RCode: RCodeNOERROR, Answer: []dns.RR{aRR(dns.Fqdn(q.Name), "192.0.2.1")}}, nil
}

// newFallback — цепь из фейков с общим регистратором вызовов.
func newFallback(t *testing.T, fakes ...*fbFake) (*FallbackResolver, *fbCalls) {
	t.Helper()
	if len(fakes) == 0 {
		t.Fatal("newFallback: нужна хотя бы основная цель")
	}
	calls := &fbCalls{}
	targets := make([]FallbackTarget, len(fakes))
	for i, f := range fakes {
		f.name = fmt.Sprintf("10.0.0.%d:53", i+1)
		f.calls = calls
		targets[i] = FallbackTarget{Addr: f.name, Resolver: f}
	}
	r, err := NewFallbackResolver(targets[0], targets[1:]...)
	if err != nil {
		t.Fatalf("NewFallbackResolver: %v", err)
	}
	return r, calls
}

// --- условие переключения ----------------------------------------------------

// TestFallbackPrimaryAnswers — основной ответил: резервы не опрашиваются,
// итог — результат основного, счётчики молчат.
func TestFallbackPrimaryAnswers(t *testing.T) {
	primary := &fbFake{res: &Result{RCode: RCodeNOERROR, Answer: []dns.RR{aRR("example.com.", "192.0.2.7")}}}
	r, calls := newFallback(t, primary, &fbFake{}, &fbFake{})

	res, err := r.Exchange(context.Background(), Question{Name: "example.com", Type: dns.TypeA})
	if err != nil {
		t.Fatalf("Exchange: %v", err)
	}
	if got := res.RecordsOf(dns.TypeA); len(got) != 1 || got[0].(*dns.A).A.String() != "192.0.2.7" {
		t.Fatalf("итог — не результат основного резолвера: %+v", res)
	}
	if order := calls.snapshot(); len(order) != 1 {
		t.Fatalf("резервы не должны опрашиваться, вызовы: %v", order)
	}
	if st := r.Stats(); st.Switches != 0 || st.Exhausted != 0 {
		t.Fatalf("счётчики при живом основном: %+v", st)
	}
}

// TestFallbackRCODEIsNotASwitch — RCODE (включая SERVFAIL/NXDOMAIN) —
// ответ сервера, а не транспортный отказ: переключения нет, семантика
// классификации ответов (T-036/T-039) не меняется.
func TestFallbackRCODEIsNotASwitch(t *testing.T) {
	for _, rcode := range []RCode{RCodeSERVFAIL, RCodeNXDOMAIN, RCodeREFUSED} {
		t.Run(rcode.String(), func(t *testing.T) {
			primary := &fbFake{res: &Result{RCode: rcode}}
			r, calls := newFallback(t, primary, &fbFake{})
			res, err := r.Exchange(context.Background(), Question{Name: "example.com", Type: dns.TypeA})
			if err != nil {
				t.Fatalf("RCODE — не ошибка: %v", err)
			}
			if res.RCode != rcode {
				t.Fatalf("rcode итога %v, ожидался %v без переключения", res.RCode, rcode)
			}
			if order := calls.snapshot(); len(order) != 1 {
				t.Fatalf("RCODE не повод опрашивать резервы, вызовы: %v", order)
			}
			if st := r.Stats(); st.Switches != 0 || st.Exhausted != 0 {
				t.Fatalf("счётчики при RCODE-ответе: %+v", st)
			}
		})
	}
}

// TestFallbackSwitchOnPrimaryFailure — основной исчерпал попытки на
// сетевой ошибке: вопрос уходит на резерв, итог — результат резерва
// (любой RCODE), Switches растёт, Elapsed — время всего обмена
// (включая цикл отказавшего).
func TestFallbackSwitchOnPrimaryFailure(t *testing.T) {
	primary := &fbFake{fn: func(ctx context.Context, q Question) (*Result, error) {
		time.Sleep(25 * time.Millisecond) // цикл ретраев мёртвого основного
		return nil, fakeNetErr{timeout: true}
	}}
	reserve := &fbFake{res: &Result{RCode: RCodeNXDOMAIN, Ns: []dns.RR{nsRR("example.com.", "ns1.example.com.")}}}
	r, calls := newFallback(t, primary, reserve)

	res, err := r.Exchange(context.Background(), Question{Name: "example.com", Type: dns.TypeA})
	if err != nil {
		t.Fatalf("резерв обязан ответить: %v", err)
	}
	if res.RCode != RCodeNXDOMAIN || len(res.Ns) != 1 {
		t.Fatalf("итог — не результат резерва: %+v", res)
	}
	if min := 25 * time.Millisecond; res.Elapsed < min {
		t.Fatalf("Elapsed %s не включает цикл отказавшего основного (< %s)", res.Elapsed, min)
	}
	if order := calls.snapshot(); len(order) != 2 || order[0] != "10.0.0.1:53" || order[1] != "10.0.0.2:53" {
		t.Fatalf("порядок цепи: %v", order)
	}
	if st := r.Stats(); st.Switches != 1 || st.Exhausted != 0 {
		t.Fatalf("счётчики после переключения: %+v", st)
	}
}

// TestFallbackSecondReserveAnswers — резервы опрашиваются по порядку
// до первого ответа.
func TestFallbackSecondReserveAnswers(t *testing.T) {
	r, calls := newFallback(t,
		&fbFake{err: fakeNetErr{timeout: true}},
		&fbFake{err: errors.New("connection refused")},
		&fbFake{},
	)

	res, err := r.Exchange(context.Background(), Question{Name: "example.com", Type: dns.TypeA})
	if err != nil {
		t.Fatalf("Exchange: %v", err)
	}
	if res.RCode != RCodeNOERROR {
		t.Fatalf("итог не от второго резерва: %+v", res)
	}
	if order := calls.snapshot(); len(order) != 3 {
		t.Fatalf("должны быть опрошены все до первого ответа, вызовы: %v", order)
	}
	if st := r.Stats(); st.Switches != 1 || st.Exhausted != 0 {
		t.Fatalf("счётчики: %+v", st)
	}
}

// --- исчерпание цепи ---------------------------------------------------------

// TestFallbackAllDead — не ответил никто: ошибка перечисляет все адреса
// цепи, оборачивает причину последнего (errors.Is, IsTimeout) и растит
// Exhausted — каждый такой обмен у потребителя станет temp_error.
func TestFallbackAllDead(t *testing.T) {
	first := errors.New("wire cut")
	last := fmt.Errorf("udp: %w", fakeNetErr{timeout: true})
	r, _ := newFallback(t,
		&fbFake{err: first},
		&fbFake{err: errors.New("no route")},
		&fbFake{err: last},
	)

	_, err := r.Exchange(context.Background(), Question{Name: "example.com", Type: dns.TypeA})
	if err == nil {
		t.Fatal("исчерпание цепи — ошибка")
	}
	for _, want := range []string{"10.0.0.1:53", "10.0.0.2:53", "10.0.0.3:53", "не ответил ни один резолвер", "example.com"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("сообщение без %q: %v", want, err)
		}
	}
	if !errors.Is(err, last) || errors.Is(err, first) {
		t.Fatalf("ошибка обязана оборачивать причину последнего резолвера и только её: %v", err)
	}
	if !IsTimeout(err) {
		t.Fatalf("классификация таймаута последнего резолвера потеряна: %v", err)
	}
	if st := r.Stats(); st.Exhausted != 1 || st.Switches != 0 {
		t.Fatalf("счётчики после исчерпания: %+v", st)
	}
}

// TestFallbackNoReserves — цепь из одного основного (пустой
// dns.fallback_resolvers): деградирует в обычный резолвер.
func TestFallbackNoReserves(t *testing.T) {
	r, _ := newFallback(t, &fbFake{err: errors.New("down")})
	if _, err := r.Exchange(context.Background(), Question{Name: "example.com", Type: dns.TypeA}); err == nil {
		t.Fatal("без резервов отказ основного — ошибка")
	}
	if st := r.Stats(); st.Exhausted != 1 || st.Switches != 0 {
		t.Fatalf("счётчики цепи без резервов: %+v", st)
	}
}

// --- отмена ctx --------------------------------------------------------------

// TestFallbackCancelledNoReserves — отмена ctx (shutdown) — не отказ
// резолвера: резервы не опрашиваются, возвращается ошибка отмены,
// счётчики чисты (деградацией не считается).
func TestFallbackCancelledNoReserves(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	primary := &fbFake{fn: func(ctx context.Context, q Question) (*Result, error) {
		cancel() // Client при отмене возвращает raw ctx-ошибку
		return nil, ctx.Err()
	}}
	r, calls := newFallback(t, primary, &fbFake{})

	_, err := r.Exchange(ctx, Question{Name: "example.com", Type: dns.TypeA})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ожидается context.Canceled, получено %v", err)
	}
	if order := calls.snapshot(); len(order) != 1 {
		t.Fatalf("отмена не должна гнать вопрос по резервам, вызовы: %v", order)
	}
	if st := r.Stats(); st.Switches != 0 || st.Exhausted != 0 {
		t.Fatalf("счётчики при отмене: %+v", st)
	}
}

// TestFallbackCancelledMidChain — отмена пришла на резерве: цепь
// прерывается сразу, без опроса остальных.
func TestFallbackCancelledMidChain(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	r, calls := newFallback(t,
		&fbFake{err: fakeNetErr{timeout: true}},
		&fbFake{fn: func(ctx context.Context, q Question) (*Result, error) {
			cancel()
			return nil, ctx.Err()
		}},
		&fbFake{},
	)

	_, err := r.Exchange(ctx, Question{Name: "example.com", Type: dns.TypeA})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ожидается context.Canceled, получено %v", err)
	}
	if order := calls.snapshot(); len(order) != 2 {
		t.Fatalf("цепь обязана прерваться на отмене, вызовы: %v", order)
	}
}

// --- сборка и конкурентность -------------------------------------------------

// TestNewFallbackResolverValidation — nil-элементы цепи — программная
// ошибка сборки.
func TestNewFallbackResolverValidation(t *testing.T) {
	if _, err := NewFallbackResolver(FallbackTarget{Addr: "a"}); err == nil {
		t.Fatal("nil-основной: ожидается ошибка")
	}
	if _, err := NewFallbackResolver(
		FallbackTarget{Addr: "a", Resolver: &fbFake{}},
		FallbackTarget{Addr: "b"},
	); err == nil {
		t.Fatal("nil-резерв: ожидается ошибка")
	}
}

// TestFallbackConcurrentStats — параллельные обмены через одну цепь
// (основной отказывает, резерв отвечает) дают согласованные счётчики;
// порядок вызовов не детерминирован — важны суммы (гонки ловит -race).
func TestFallbackConcurrentStats(t *testing.T) {
	r, calls := newFallback(t,
		&fbFake{err: fakeNetErr{timeout: true}},
		&fbFake{},
	)
	const total = 40
	var wg sync.WaitGroup
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := r.Exchange(context.Background(), Question{Name: fmt.Sprintf("h%d.example.com", i), Type: dns.TypeA}); err != nil {
				t.Errorf("Exchange(%d): %v", i, err)
			}
		}(i)
	}
	wg.Wait()
	if st := r.Stats(); st.Switches != int64(total) || st.Exhausted != 0 {
		t.Fatalf("счётчики: Switches=%d Exhausted=%d, ожидалось %d/0", st.Switches, st.Exhausted, total)
	}
	if n := len(calls.snapshot()); n != total*2 {
		t.Fatalf("вызовов %d, ожидалось %d (основной + резерв на каждый обмен)", n, total*2)
	}
}

// Тесты цепи резолверов живой проверки (см. resolve.go): сети нет —
// элементы цепи, как в fallback_test.go (dnsclient), фейки Resolver с
// программируемым итогом и подсчётом вызовов. Проверяются: условие
// эскалации (SERVFAIL/REFUSED и транспортный отказ — да; NOERROR и
// NXDOMAIN — нет), порядок цепи, итог «никто не ответил полезным»
// (последний RCODE либо агрегированная ошибка), отмена ctx и
// конкурентность (гонки ловит -race).

package checker

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/miekg/dns"

	"crawler/internal/dnsclient"
)

// resFake — элемент цепи: возвращает заданный RCODE/ошибку; calls
// (если задан) считает вызовы по имени.
type resFake struct {
	name  string
	calls *sync.Map
	rcode dnsclient.RCode
	err   error
}

func (f *resFake) Exchange(context.Context, dnsclient.Question) (*dnsclient.Result, error) {
	if f.calls != nil {
		if v, ok := f.calls.Load(f.name); ok {
			p := v.(*int)
			*p++
		}
	}
	if f.err != nil {
		return nil, f.err
	}
	return &dnsclient.Result{RCode: f.rcode}, nil
}

func newCalls(names ...string) *sync.Map {
	m := &sync.Map{}
	for _, n := range names {
		zero := 0
		m.Store(n, &zero)
	}
	return m
}

func called(m *sync.Map, name string) int {
	v, ok := m.Load(name)
	if !ok {
		return -1
	}
	return *v.(*int)
}

func chainOf(targets ...escalateTarget) *escalateResolver {
	return &escalateResolver{targets: targets}
}

func q() dnsclient.Question {
	return dnsclient.Question{Name: "example.com.", Type: dns.TypeA}
}

// TestEscalateUsefulFirstAnswer — полезный итог первого элемента
// возвращается как есть, резервы не опрашиваются: NOERROR (включая
// пустой NODATA) и NXDOMAIN (имени нет — законный ответ, не отказ
// сервера).
func TestEscalateUsefulFirstAnswer(t *testing.T) {
	cases := []struct {
		name  string
		rcode dnsclient.RCode
	}{
		{"NOERROR", dnsclient.RCodeNOERROR},
		{"NXDOMAIN", dnsclient.RCodeNXDOMAIN},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			calls := newCalls("p", "f")
			r := chainOf(
				escalateTarget{addr: "p", res: &resFake{name: "p", calls: calls, rcode: tc.rcode}},
				escalateTarget{addr: "f", res: &resFake{name: "f", calls: calls, rcode: dnsclient.RCodeNOERROR}},
			)
			res, err := r.Exchange(context.Background(), q())
			if err != nil {
				t.Fatalf("неожиданная ошибка: %v", err)
			}
			if res.RCode != tc.rcode {
				t.Fatalf("итог = %s, ожидался %s", res.RCode, tc.rcode)
			}
			if n := called(calls, "f"); n != 0 {
				t.Fatalf("резерв опрошен (%d раз) при полезном итоге основного", n)
			}
		})
	}
}

// TestEscalateOnRCODEFailure — SERVFAIL и REFUSED основного — отказ
// сервера: цепь переходит к резерву, его полезный итог возвращается.
func TestEscalateOnRCODEFailure(t *testing.T) {
	cases := []struct {
		name     string
		primary  dnsclient.RCode
		expected dnsclient.RCode
	}{
		{"SERVFAIL→NOERROR", dnsclient.RCodeSERVFAIL, dnsclient.RCodeNOERROR},
		{"REFUSED→NXDOMAIN", dnsclient.RCodeREFUSED, dnsclient.RCodeNXDOMAIN},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			calls := newCalls("p", "f")
			r := chainOf(
				escalateTarget{addr: "p", res: &resFake{name: "p", calls: calls, rcode: tc.primary}},
				escalateTarget{addr: "f", res: &resFake{name: "f", calls: calls, rcode: tc.expected}},
			)
			res, err := r.Exchange(context.Background(), q())
			if err != nil {
				t.Fatalf("неожиданная ошибка: %v", err)
			}
			if res.RCode != tc.expected {
				t.Fatalf("итог = %s, ожидался %s", res.RCode, tc.expected)
			}
			if n := called(calls, "f"); n != 1 {
				t.Fatalf("резерв опрошен %d раз, ожидался 1", n)
			}
		})
	}
}

// TestEscalateOnTransportError — транспортный отказ основного (error
// после ретраев) — то же переключение, что и у FallbackResolver T-107.
func TestEscalateOnTransportError(t *testing.T) {
	calls := newCalls("p", "f")
	r := chainOf(
		escalateTarget{addr: "p", res: &resFake{name: "p", calls: calls, err: errors.New("i/o timeout")}},
		escalateTarget{addr: "f", res: &resFake{name: "f", calls: calls, rcode: dnsclient.RCodeNOERROR}},
	)
	res, err := r.Exchange(context.Background(), q())
	if err != nil {
		t.Fatalf("неожиданная ошибка: %v", err)
	}
	if res.RCode != dnsclient.RCodeNOERROR {
		t.Fatalf("итог = %s, ожидался NOERROR", res.RCode)
	}
	if n := called(calls, "f"); n != 1 {
		t.Fatalf("резерв опрошен %d раз, ожидался 1", n)
	}
}

// TestEscalateAllFailedRCODE — все элементы ответали RCODE-отказом:
// без ошибки возвращается последний RCODE-ответ (потребитель честно
// классифицирует SERVFAIL → temp_error).
func TestEscalateAllFailedRCODE(t *testing.T) {
	calls := newCalls("p", "f1", "f2")
	r := chainOf(
		escalateTarget{addr: "p", res: &resFake{name: "p", calls: calls, rcode: dnsclient.RCodeSERVFAIL}},
		escalateTarget{addr: "f1", res: &resFake{name: "f1", calls: calls, rcode: dnsclient.RCodeSERVFAIL}},
		escalateTarget{addr: "f2", res: &resFake{name: "f2", calls: calls, rcode: dnsclient.RCodeREFUSED}},
	)
	res, err := r.Exchange(context.Background(), q())
	if err != nil {
		t.Fatalf("RCODE-отказ — не ошибка цепи: %v", err)
	}
	if res.RCode != dnsclient.RCodeREFUSED {
		t.Fatalf("итог = %s, ожидался последний REFUSED", res.RCode)
	}
	for _, name := range []string{"p", "f1", "f2"} {
		if n := called(calls, name); n != 1 {
			t.Fatalf("%s опрошен %d раз, ожидался 1", name, n)
		}
	}
}

// TestEscalateAllTransportFailed — сплошные транспортные отказы:
// ошибка перечисляет всю цепь и оборачивает причину последнего.
func TestEscalateAllTransportFailed(t *testing.T) {
	boom := errors.New("connection refused")
	r := chainOf(
		escalateTarget{addr: "p", res: &resFake{name: "p", err: errors.New("i/o timeout")}},
		escalateTarget{addr: "f", res: &resFake{name: "f", err: boom}},
	)
	_, err := r.Exchange(context.Background(), q())
	if err == nil {
		t.Fatal("ожидалась ошибка исчерпания цепи")
	}
	for _, sub := range []string{"p:", "f:", "connection refused", "i/o timeout"} {
		if !strings.Contains(err.Error(), sub) {
			t.Fatalf("в сообщении нет %q: %v", sub, err)
		}
	}
	if !errors.Is(err, boom) {
		t.Fatal("errors.Is не находит причину последнего резолвера")
	}
}

// TestEscalateContextCancelled — отмена ctx между элементами цепи
// прерывает её сразу (shutdown не обстреливает резервы).
func TestEscalateContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := chainOf(
		escalateTarget{addr: "p", res: &resFake{name: "p", err: errors.New("i/o timeout")}},
		escalateTarget{addr: "f", res: &resFake{name: "f", rcode: dnsclient.RCodeNOERROR}},
	)
	_, err := r.Exchange(ctx, q())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ожидалась ошибка отмены, получено: %v", err)
	}
}

// TestEscalateConcurrent — цепь без состояния: параллельные Exchange
// не мешают друг другу (гонки ловит -race).
func TestEscalateConcurrent(t *testing.T) {
	r := chainOf(
		escalateTarget{addr: "p", res: &resFake{name: "p", rcode: dnsclient.RCodeSERVFAIL}},
		escalateTarget{addr: "f", res: &resFake{name: "f", rcode: dnsclient.RCodeNOERROR}},
	)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := r.Exchange(context.Background(), q())
			if err != nil {
				t.Errorf("параллельный обмен: %v", err)
				return
			}
			if res.RCode != dnsclient.RCodeNOERROR {
				t.Errorf("параллельный обмен: итог %s, ожидался NOERROR", res.RCode)
			}
		}()
	}
	wg.Wait()
}

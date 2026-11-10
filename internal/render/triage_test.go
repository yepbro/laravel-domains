package render

// Триаж выживших мутантов CI-прогона части C (render, 10.11.2026,
// score 0.916 — 54 выживших): поведенческие дыры закрываются тестами
// этого файла и дополнением таблиц TestWhatwgParseIPv4 и
// TestOptionsValidate в render_test.go; доказуемо эквивалентные мутанты
// погашены блэклистом (test/mutation/blacklist.txt, группы обоснований —
// docs/mutation-testing.md §4). Тесты герметичны (швы LookupHost,
// procRSS/timeNow, фейковое время кэша) и не спят дольше 50 мс на обмен:
// сюит пакета исполняется на каждого мутанта в CI и лишних секунд
// себе позволять не может.

import (
	"context"
	"math"
	"net/netip"
	"testing"
	"time"

	"github.com/chromedp/chromedp"
)

// TestWhatwgIPv4Number — прямой контракт числа одной части IPv4-хоста
// WHATWG: обе шестнадцатеричные приставки (0x/0X) равноправны, формы
// держат полную ширину uint64 (граница 2^63/maxuint64 — bitSize 64, а
// не «почти 64»), посторонний символ — не число (hex-детект по s[1] без
// проверки ведущего нуля принимал бы «9x» за пустую мантиссу).
func TestWhatwgIPv4Number(t *testing.T) {
	cases := []struct {
		s    string
		want uint64
		ok   bool
	}{
		{"0x7f", 0x7f, true},
		{"0X1F", 31, true},
		{"0x", 0, true},
		{"0x8000000000000000", 1 << 63, true},
		{"01000000000000000000000", 1 << 63, true},
		{"18446744073709551615", math.MaxUint64, true},
		{"010", 8, true},
		{"9x", 0, false},
		{"", 0, false},
		{"09", 0, false},
	}
	for _, tc := range cases {
		v, ok := whatwgIPv4Number(tc.s)
		if ok != tc.ok || (ok && v != tc.want) {
			t.Errorf("whatwgIPv4Number(%q) = %d %v, хочу %d %v",
				tc.s, v, ok, tc.want, tc.ok)
		}
	}
}

// TestValidateTaskURLInternalTriage — контракты адресного контроля
// задачи (T-219): неразбираемый URL и пустой хост уже отвергнуты
// валидацией схемы (validateTaskURL) — адресной проверки нет (nil) и
// паники на нил-URL быть не должно; внутренний числоподобный хост —
// task-ошибка до перехода на страницу.
func TestValidateTaskURLInternalTriage(t *testing.T) {
	for _, raw := range []string{"http://[::1", "http:///path", "not a url"} {
		if err := validateTaskURLInternal(raw); err != nil {
			t.Errorf("%q: хочу nil (схема/хост отвергнуты раньше), получено %v", raw, err)
		}
	}
	err := validateTaskURLInternal("http://127.1/")
	if err == nil || ClassOf(err) != ClassTask {
		t.Fatalf("внутренний числоподобный хост: хочу task-ошибку, получено %v", err)
	}
}

// TestRequestURLInternalTriage — литеральные формы хоста не резолвятся
// вовсе (адрес известен без DNS-обмена): внешний IPv6-литерал (мимо
// числоподобной ветки WHATWG) и внешняя десятичная числоподобная форма
// («134744072» = 8.8.8.8) дают вердикт без единого обращения к
// резолверу; неразбираемый URL — не внутренний, без паники. Имя-хост
// доходит до резолва ровно один раз.
func TestRequestURLInternalTriage(t *testing.T) {
	lookup := &fakeLookupHost{addrs: map[string][]netip.Addr{
		"fallback.example.net": {netip.MustParseAddr("203.0.113.7")},
	}}
	dns := newDNSCache(lookup.lookup)
	ctx := context.Background()

	if requestURLInternal(ctx, "http://[::1", dns) {
		t.Error("неразбираемый URL: хочу не-внутренний")
	}
	if requestURLInternal(ctx, "http://[2001:db8::1]/", dns) {
		t.Error("внешний IPv6-литерал: хочу не-внутренний")
	}
	if requestURLInternal(ctx, "http://134744072/", dns) {
		t.Error("внешняя числоподобная форма: хочу не-внутренний")
	}
	if n := len(lookup.callsOf()); n != 0 {
		t.Fatalf("обменов резолвера %d (%v), хочу 0 — литерал не резолвится",
			n, lookup.callsOf())
	}

	if requestURLInternal(ctx, "http://fallback.example.net/", dns) {
		t.Error("имя с публичным резолвом: хочу не-внутренний")
	}
	if n := len(lookup.callsOf()); n != 1 {
		t.Fatalf("обменов резолвера %d, хочу 1 (имя резолвится)", n)
	}
}

// TestDNSCacheDefaultsTriage — швы и продуктовые константы кэша
// резолвов (T-230): нулевой LookupHost подменяется системным
// резолвером, лениво достраивается шов времени, потолок обмена —
// ровно 3 с (докомментарий dnsLookupTimeout: выше холодной рекурсии
// unbound, ниже таймаута страницы). Кэш без предустановленного now
// работает и кэширует вердикт.
func TestDNSCacheDefaultsTriage(t *testing.T) {
	c := newDNSCache(nil)
	if c.lookup == nil {
		t.Fatal("нулевой шов резолва: хочу системный резолвер (lookupHostSystem)")
	}
	if c.now == nil {
		t.Fatal("шов времени не установлен (хочу time.Now по умолчанию)")
	}
	if c.timeout != 3*time.Second {
		t.Fatalf("потолок обмена %s, хочу ровно 3с (dnsLookupTimeout)", c.timeout)
	}

	c2 := &dnsCache{
		lookup: (&fakeLookupHost{addrs: map[string][]netip.Addr{
			"rebind.example.net": {netip.MustParseAddr("169.254.169.254")},
		}}).lookup,
		timeout: time.Hour,
	}
	if !c2.hostInternal(context.Background(), "rebind.example.net") {
		t.Fatal("первый резолв: хочу внутренний (169.254.169.254)")
	}
	if !c2.hostInternal(context.Background(), "rebind.example.net") {
		t.Fatal("повтор: хочу внутренний (вердикт из кэша)")
	}
	if len(c2.entries) != 1 {
		t.Fatalf("записей кэша %d, хочу 1 (вердикт закэширован)", len(c2.entries))
	}
}

// TestDNSCacheExchangeTimeoutTriage — граничные потолки одного обмена
// (T-230): timeout == 0 означает «без обёртки дедлайна» — резолв идёт
// напрямую и его ответ (внутренний адрес) становится вердиктом;
// положительный потолок ниже времени ответа обрезает обмен — вердикт
// fail-open и не кэшируется. Резолвер уважает контекст обмена; запас
// 50 мс на ответ против наносекундного дедлайна снимает зависимость
// от таймингов планировщика.
func TestDNSCacheExchangeTimeoutTriage(t *testing.T) {
	lookup := func(ctx context.Context, _ string) ([]netip.Addr, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(50 * time.Millisecond):
			return []netip.Addr{netip.MustParseAddr("169.254.169.254")}, nil
		}
	}

	direct := newDNSCache(lookup)
	direct.timeout = 0
	if !direct.hostInternal(context.Background(), "rebind.example.net") {
		t.Fatal("timeout=0: прямой резолв без дедлайна — хочу внутренний вердикт")
	}
	if len(direct.entries) != 1 {
		t.Fatalf("timeout=0: записей %d, хочу 1 (успех кэшируется)", len(direct.entries))
	}

	cut := newDNSCache(lookup)
	cut.timeout = 1 // 1 нс: положительный потолок — обмен обрезается
	if cut.hostInternal(context.Background(), "rebind.example.net") {
		t.Fatal("timeout=1ns: хочу fail-open (дедлайн обгоняет обмен)")
	}
	if len(cut.entries) != 0 {
		t.Fatalf("timeout=1ns: записей %d, хочу 0 (обрыв не кэшируется)", len(cut.entries))
	}
}

// TestDNSCacheTTLBoundaryTriage — границы TTL кэша резолвов: запись
// возрастом 29,5 с ещё свежа (вердикт из кэша, без обмена), возрастом
// ровно 30 с — истекла (свежий обмен берёт новый ответ DNS). Литералы
// пинят продуктовые 30 с (dnsCacheTTL): срез границ — то, что держит
// контур DNS-rebinding (T-230) от залипания вердикта дольше окна.
func TestDNSCacheTTLBoundaryTriage(t *testing.T) {
	addrs := []netip.Addr{netip.MustParseAddr("169.254.169.254")}
	calls := 0
	c := newDNSCache(func(_ context.Context, _ string) ([]netip.Addr, error) {
		calls++
		return addrs, nil
	})
	now := time.Now()
	c.now = func() time.Time { return now }

	if !c.hostInternal(context.Background(), "rebind.example.net") {
		t.Fatal("первый обмен: хочу внутренний")
	}

	// Возраст 29,5 с < 30 с: кэш свеж — новый ответ DNS не виден.
	addrs = []netip.Addr{netip.MustParseAddr("203.0.113.7")}
	now = now.Add(29500 * time.Millisecond)
	if !c.hostInternal(context.Background(), "rebind.example.net") {
		t.Fatal("возраст 29,5с: хочу внутренний из кэша")
	}
	if calls != 1 {
		t.Fatalf("обменов %d, хочу 1 (свежая запись — без обмена)", calls)
	}

	// Возраст ровно 30 с: истёк — свежий обмен несёт внешний ответ.
	now = now.Add(500 * time.Millisecond)
	if c.hostInternal(context.Background(), "rebind.example.net") {
		t.Fatal("возраст 30с: хочу внешний по свежему обмену")
	}
	if calls != 2 {
		t.Fatalf("обменов %d, хочу 2 (истёкшая запись обновляется)", calls)
	}
}

// TestDNSCacheCanceledNotCachedTriage — обрыв обмена по отмене контекста
// (конец страницы) — не ответ DNS: вердикт fail-open, но НЕ кэшируется;
// повторный запрос того же хоста обменивается заново и видит уже
// внутренний адрес (контур не замирает на «внешнем» после обрыва).
func TestDNSCacheCanceledNotCachedTriage(t *testing.T) {
	canceled := false
	c := newDNSCache(func(_ context.Context, _ string) ([]netip.Addr, error) {
		if canceled {
			return nil, context.Canceled
		}
		return []netip.Addr{netip.MustParseAddr("169.254.169.254")}, nil
	})
	now := time.Now()
	c.now = func() time.Time { return now }

	canceled = true
	if c.hostInternal(context.Background(), "rebind.example.net") {
		t.Fatal("обрыв по отмене: хочу fail-open (не внутренний)")
	}
	if len(c.entries) != 0 {
		t.Fatalf("записей %d, хочу 0 (обрыв не кэшируется)", len(c.entries))
	}

	canceled = false
	if !c.hostInternal(context.Background(), "rebind.example.net") {
		t.Fatal("повтор после обрыва: свежий обмен — внутренний адрес")
	}
}

// TestProbeConnTriage — контракты зонда соединения (T-218/T-226):
// нулевой браузер и залипшая потеря соединения не зондируются (false),
// браузер юнит-тестов без chromedp-контекста и контекст без браузерной
// сессии (Browser == nil до первого Run) — зонду нечего проверять,
// вердикт «жив» без похода в транспорт.
func TestProbeConnTriage(t *testing.T) {
	var nilBrowser *Browser
	if nilBrowser.probeConn() {
		t.Error("nil-браузер: зонд не проходит")
	}

	lost := testBrowser(1)
	lost.lost.Store(true)
	if lost.probeConn() {
		t.Error("залипшая потеря соединения: зонд не проходит")
	}

	if !testBrowser(1).probeConn() {
		t.Error("браузер без chromedp-контекста (юнит-фейки): зонду нечего проверять — жив")
	}

	bctx, cancel := chromedp.NewContext(context.Background())
	defer cancel()
	withCtx := testBrowser(1)
	withCtx.ctx = bctx
	if !withCtx.probeConn() {
		t.Error("контекст без браузерной сессии (Browser == nil до Run): зонд не падает")
	}
}

// TestBrowserRSSCacheTriage — границы кэша RSS (T-229): кэшируются
// только положительные измерения — нулевой (корень исчез из снимка) и
// отрицательный (/proc недоступен) кэш не читается как значение и не
// записывается (свежий снимок на каждом обращении); кэш с тем же
// корнем, нулевым/отрицательным значением и свежей меткой — не кэш;
// гранично малое положительное измерение (1 Б) — валидное значение и
// пишется, и читается из кэша.
func TestBrowserRSSCacheTriage(t *testing.T) {
	oldProc, oldNow := procRSS, timeNow
	fakeNow := time.Now()
	var rss int64
	procRSS = func(int) int64 { return rss }
	timeNow = func() time.Time { return fakeNow }
	t.Cleanup(func() { procRSS, timeNow = oldProc, oldNow })

	// Нулевой и отрицательный кэш того же корня со свежей меткой —
	// не значения: обращение уходит в измерение.
	rss = 777
	zero := &Browser{rootPID: 1234, rssPID: 1234, rss: 0, rssAt: fakeNow}
	if got := zero.RSS(); got != 777 {
		t.Fatalf("нулевой кэш: RSS = %d, хочу 777 (свежий снимок)", got)
	}
	negative := &Browser{rootPID: 1234, rssPID: 1234, rss: -1, rssAt: fakeNow}
	if got := negative.RSS(); got != 777 {
		t.Fatalf("отрицательный кэш: RSS = %d, хочу 777 (свежий снимок)", got)
	}

	// Измерение 0 (корень исчез) и -1 (/proc недоступен) — не
	// значения, а их отсутствие: не кэшируются (метка не пишется).
	rss = 0
	gone := &Browser{rootPID: 4242424}
	if got := gone.RSS(); got != 0 {
		t.Fatalf("исчезнувший корень: RSS = %d, хочу 0", got)
	}
	if !gone.rssAt.IsZero() || gone.rssPID != 0 {
		t.Fatalf("нулевой снимок закэширован (rssAt=%v, rssPID=%d) — кэш только положительных",
			gone.rssAt, gone.rssPID)
	}
	rss = -1
	noProc := &Browser{rootPID: 4242424}
	if got := noProc.RSS(); got != -1 {
		t.Fatalf("недоступный /proc: RSS = %d, хочу -1", got)
	}
	if !noProc.rssAt.IsZero() {
		t.Fatalf("снимок -1 закэширован (rssAt=%v) — кэш только положительных", noProc.rssAt)
	}

	// Граничное положительное 1 Б: пишется в кэш и читается из него
	// (второе обращение не измеряет заново).
	rss = 1
	one := &Browser{rootPID: 1234}
	if got := one.RSS(); got != 1 {
		t.Fatalf("первое чтение: RSS = %d, хочу 1", got)
	}
	rss = 2
	if got := one.RSS(); got != 1 {
		t.Fatalf("второе чтение: RSS = %d, хочу 1 из кэша", got)
	}
}

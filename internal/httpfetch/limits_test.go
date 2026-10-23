// Тесты лимитов T-049. Уровни:
//
//   - чистые единичные: математика дедлайна анти-тарпита, токен-ведро
//     (темп), семафоры per-IP и глобальный (очередь, отмена, очистка
//     карты), опции из конфигурации и дефолты; override-лимиты per-IP
//     по префиксам и наблюдение ожидания слота (T-165);
//   - httptest-серверы на петле: потолок тела (ровно в лимит / сверх —
//     truncated, в т.ч. chunked), тарпит тела (нарушение скорости после
//     первого байта), темп без ложных срабатываний, лимит соединений
//     на один IP (третий запрос ждёт очереди), отмена в очереди,
//     темп RoundTrip, keep-alive (соединение не переиспользуется);
//   - переиспользуются помощники transport_test.go: loopbackAddr,
//     hostURL, stallServer.

package httpfetch

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"crawler/internal/config"
)

// --- опции и математика ------------------------------------------------------

// TestLimitOptionsFromConfigAndDefaults: перенос полей секции http,
// дефолты таблицы лимитов ТЗ 5.5, включение лимитов в NewOptions.
func TestLimitOptionsFromConfigAndDefaults(t *testing.T) {
	cfg := config.HTTP{
		Concurrency:    777,
		PerIPLimit:     3,
		PerIPOverrides: map[string]int{"195.208.9.9/16": 12},
		MaxBodyBytes:   config.ByteSize(3 << 20),
		MinBodySpeed:   config.ByteSize(4096),
		MaxRPS:         500,
		MaxRedirects:   8,
		UserAgent:      "t",
		ConnectTimeout: config.Duration(time.Second),
	}
	lo := NewLimitOptions(cfg)
	if lo.MaxBodyBytes != 3<<20 || lo.MinBodySpeed != 4096 || lo.PerIPConns != 3 ||
		lo.GlobalConns != 777 || lo.MaxRPS != 500 {
		t.Errorf("перенос из секции http: %+v", lo)
	}
	// Карта префиксов (T-165) разбирается в override-лимиты
	// (нормализация маскированием); мусор в ручной сборке отключает
	// список целиком (валидный путь отсекает мусор раньше).
	if len(lo.PerIPOverrides) != 1 {
		t.Fatalf("override-лимиты: %v, want один префикс", lo.PerIPOverrides)
	}
	if o := lo.PerIPOverrides[0]; o.Prefix.String() != "195.208.0.0/16" || o.Limit != 12 {
		t.Errorf("override = %s:%d, want 195.208.0.0/16:12", o.Prefix, o.Limit)
	}
	bad := NewLimitOptions(config.HTTP{PerIPLimit: 3, PerIPOverrides: map[string]int{"мусор": 8}})
	if bad.PerIPOverrides != nil {
		t.Errorf("мусорная карта: %v, want nil (отключение целиком)", bad.PerIPOverrides)
	}

	// NewOptions включает лимиты всегда (бойовый транспорт — с лимитами).
	o := NewOptions(cfg)
	if o.Limits == nil {
		t.Fatal("NewOptions: лимиты не включены")
	}
	if o.Limits.MaxBodyBytes != 3<<20 || o.Limits.MaxRPS != 500 {
		t.Errorf("лимиты NewOptions: %+v", o.Limits)
	}

	// Дефолты: таблица лимитов ТЗ 5.5 + выведенные (RPS, окно скорости).
	d := LimitOptions{}.withDefaults()
	if d.MaxBodyBytes != DefaultLimitBodyBytes || d.MinBodySpeed != DefaultLimitBodySpeed ||
		d.MinSpeedWindow != DefaultLimitSpeedWindow || d.PerIPConns != DefaultLimitPerIPConns ||
		d.GlobalConns != DefaultLimitGlobalConns || d.MaxRPS != DefaultLimitRPS {
		t.Errorf("дефолты лимитов: %+v", d)
	}
	if d.Burst != d.GlobalConns {
		t.Errorf("дефолт всплеска: %d, ожидался GlobalConns=%d", d.Burst, d.GlobalConns)
	}
}

// TestBodyLimiterDeadline: дедлайн дочитания остатка — now+remain/speed,
// но не короче окна-пола; выключенная скорость — без дедлайна.
func TestBodyLimiterDeadline(t *testing.T) {
	b := &BodyLimiter{speed: 2048, window: 500 * time.Millisecond}
	if d := b.deadlineLocked(4096); d != 2*time.Second {
		t.Errorf("дедлайн остатка 4096 байт при 2048 байт/с: %s, ожидалось 2s", d)
	}
	if d := b.deadlineLocked(100); d != 500*time.Millisecond {
		t.Errorf("короткий остаток без пола окна: %s, ожидалось 500ms", d)
	}
	b.speed = 0
	if d := b.deadlineLocked(4096); d != 0 {
		t.Errorf("выключенный анти-тарпит не должен давать дедлайн: %s", d)
	}
	// Большой остаток при скорости 1 байт/с не переполняет time.Duration.
	b.speed = 1
	if d := b.deadlineLocked(2 << 20); d <= 0 {
		t.Errorf("большой остаток: неожиданный дедлайн %s", d)
	}
}

// TestBodyLimiterClosedRead: чтение после Close — ошибка.
func TestBodyLimiterClosedRead(t *testing.T) {
	b := newBodyLimiter(io.NopCloser(strings.NewReader("abc")), func() {},
		LimitOptions{MaxBodyBytes: 10, MinBodySpeed: 1, MinSpeedWindow: time.Hour})
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := b.Read(make([]byte, 4)); !errors.Is(err, errLimitBodyClosed) {
		t.Errorf("чтение закрытого тела: %v", err)
	}
}

// readCountingRC — источник с подсчётом Read (нулевые буферы и повторные
// чтения не должны трогать источник).
type readCountingRC struct {
	io.Reader
	reads *int
}

func (r *readCountingRC) Read(p []byte) (int, error) {
	*r.reads++
	return r.Reader.Read(p)
}

func (r *readCountingRC) Close() error { return nil }

// TestBodyLimiterZeroBufAndRepeatRead: буфер длины 0 не читает источник
// (контракт io.Reader); после классификации хвоста за потолком
// повторные чтения возвращают запомненный итог (io.EOF или ошибку),
// не перечитывая пробный байт.
func TestBodyLimiterZeroBufAndRepeatRead(t *testing.T) {
	run := func(t *testing.T, size, limit int, wantTrunc bool) {
		t.Helper()
		var reads int
		bl := newBodyLimiter(&readCountingRC{Reader: bytes.NewReader(bytes.Repeat([]byte("a"), size)), reads: &reads},
			func() {}, LimitOptions{MaxBodyBytes: int64(limit)}) // speed 0 — сторож выключен

		if n, err := bl.Read(nil); n != 0 || err != nil {
			t.Errorf("Read(nil) = (%d, %v), ожидалось (0, nil)", n, err)
		}
		if n, err := bl.Read(make([]byte, 0)); n != 0 || err != nil {
			t.Errorf("Read(пустой буфер) = (%d, %v), ожидалось (0, nil)", n, err)
		}
		if reads != 0 {
			t.Fatalf("нулевые буферы прочитали источник: %d чтений", reads)
		}

		buf := make([]byte, size+8)
		got := 0
		for got < limit && got < size {
			n, err := bl.Read(buf[got:])
			if err != nil {
				t.Fatalf("чтение до потолка: %v", err)
			}
			got += n
		}
		if _, err := bl.Read(buf); err != io.EOF {
			t.Fatalf("проба хвоста: %v, ожидался io.EOF", err)
		}
		if bl.Truncated() != wantTrunc {
			t.Errorf("признак обрезки: %v, ожидался %v", bl.Truncated(), wantTrunc)
		}
		probes := reads
		for i := 0; i < 3; i++ {
			if n, err := bl.Read(buf); n != 0 || err != io.EOF {
				t.Errorf("повторное чтение №%d: (%d, %v), ожидалось (0, io.EOF)", i+1, n, err)
			}
		}
		if reads != probes {
			t.Errorf("повторные чтения тронули источник: %d → %d чтений", probes, reads)
		}
		_ = bl.Close()
	}
	t.Run("сверх лимита", func(t *testing.T) { run(t, 10, 5, true) })
	t.Run("ровно в лимит", func(t *testing.T) { run(t, 5, 5, false) })
}

// --- токен-ведро темпа -------------------------------------------------------

// TestLimitBucketPacing: всплеск до burst уходит сразу, дальнейшие
// токены — темпом rate; ожидание прерывается отменой ctx.
func TestLimitBucketPacing(t *testing.T) {
	b := newLimitBucket(25, 2) // 25 токенов/с, всплеск 2
	start := time.Now()
	for i := 0; i < 5; i++ {
		if err := b.take(context.Background()); err != nil {
			t.Fatalf("take #%d: %v", i+1, err)
		}
	}
	elapsed := time.Since(start)
	// 2 мгновенно + 3 по 40 мс ≈ 120 мс; нижняя граница с допуском.
	if elapsed < 100*time.Millisecond {
		t.Errorf("темп ведра не выдержан: 5 токенов за %s (ожидалось ≥100ms)", elapsed)
	}
	if elapsed >= 2*time.Second {
		t.Errorf("темп ведра затянут: %s", elapsed)
	}

	// Пустое ведро: ожидание прерывается отменой ctx.
	slow := newLimitBucket(0.001, 1)
	if err := slow.take(context.Background()); err != nil {
		t.Fatalf("burst-токен: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := slow.take(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("ожидание токена не прервано отменой: %v", err)
	}
}

// --- семафоры соединений -----------------------------------------------------

// TestLimitIPMapQueueAndCancel: потолок на один IP, очередь пробуждается
// освобождением, отменённый ждущий не занимает слот, опустевшая запись
// удаляется из карты.
func TestLimitIPMapQueueAndCancel(t *testing.T) {
	m := limitIPMap{limit: 2, entries: make(map[netip.Addr]*limitIPEntry)}
	ip := netip.MustParseAddr("192.0.2.1")
	ctx := context.Background()

	r1, err := m.acquire(ctx, ip)
	if err != nil {
		t.Fatalf("слот 1: %v", err)
	}
	r2, err := m.acquire(ctx, ip)
	if err != nil {
		t.Fatalf("слот 2: %v", err)
	}

	// Третий — в очереди (не возвращается, пока слоты заняты).
	queued := make(chan error, 1)
	var release3 func()
	go func() {
		rel, err := m.acquire(ctx, ip)
		if err == nil {
			release3 = rel
		}
		queued <- err
	}()
	select {
	case err := <-queued:
		t.Fatalf("третий слот взят при полном потолке: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	// Освобождение будит ждущего.
	r1()
	select {
	case err := <-queued:
		if err != nil {
			t.Fatalf("ожидающий не получил слот: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("очередь не пробудилась освобождением слота")
	}

	// Отмена ждущего — ошибка, слот не занимается.
	cctx, cancel := context.WithCancel(ctx)
	cancelled := make(chan error, 1)
	go func() {
		_, err := m.acquire(cctx, ip)
		cancelled <- err
	}()
	// Слоты снова полны (r2 +release3): ждущий блокируется.
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-cancelled:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("отмена очереди: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("отмена не разбудила ждущего")
	}

	// Опустевшая карта чистится: записей по IP нет.
	r2()
	release3()
	m.mu.Lock()
	entries := len(m.entries)
	m.mu.Unlock()
	if entries != 0 {
		t.Errorf("записи per-IP не очищены: %d", entries)
	}
}

// TestLimitStateAcquireDial: порядок per-IP → глобальный; глобальный
// потолок ставит в очередь; отмена в очереди — ошибка; освобождение
// будит.
func TestLimitStateAcquireDial(t *testing.T) {
	st := newLimitState(LimitOptions{
		PerIPConns: 4, GlobalConns: 2, MaxRPS: 1000, Burst: 10,
		MaxBodyBytes: 1024, MinBodySpeed: 1024, MinSpeedWindow: time.Second,
	})
	ip1 := netip.MustParseAddr("192.0.2.1")
	ip2 := netip.MustParseAddr("192.0.2.2")
	ip3 := netip.MustParseAddr("192.0.2.3")
	ctx := context.Background()

	r1, err := st.acquireDial(ctx, ip1)
	if err != nil {
		t.Fatalf("соединение 1: %v", err)
	}
	r2, err := st.acquireDial(ctx, ip2)
	if err != nil {
		t.Fatalf("соединение 2: %v", err)
	}

	// Глобальный потолок исчерпан (per-IP — нет): третий в очереди.
	queued := make(chan struct{ rel func() }, 1)
	go func() {
		rel, err := st.acquireDial(ctx, ip3)
		if err != nil {
			t.Errorf("соединение 3: %v", err)
			rel = func() {}
		}
		queued <- struct{ rel func() }{rel}
	}()
	select {
	case got := <-queued:
		got.rel()
		t.Fatal("третье соединение прошло полный глобальный потолок")
	case <-time.After(100 * time.Millisecond):
	}

	// Отмена ждущего — ошибка, слоты не занимаются.
	cctx, cancel := context.WithCancel(ctx)
	cancelled := make(chan error, 1)
	go func() {
		_, err := st.acquireDial(cctx, ip3)
		cancelled <- err
	}()
	cancel()
	select {
	case err := <-cancelled:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("отмена очереди диала: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("отмена не разбудила ждущего соединения")
	}

	// Освобождение глобального слота будит очередь.
	r1()
	select {
	case got := <-queued:
		defer got.rel()
	case <-time.After(time.Second):
		t.Fatal("очередь соединений не пробудилась")
	}
	r2()
}

// --- override-лимиты per-IP и наблюдение ожидания (T-165) ---------------------

// TestParsePerIPOverrides: разбор карты «CIDR → лимит» — нормализация
// маскированием («195.208.9.9/16» — сеть 195.208.0.0/16), IPv4 и IPv6;
// ошибки: мусорный CIDR, лимит < 1, дубль сети после маскирования.
func TestParsePerIPOverrides(t *testing.T) {
	overrides, err := parsePerIPOverrides(map[string]int{
		"195.208.9.9/16": 12,
		"2a02:6b8::/32":  8,
		"192.0.2.0/24":   2,
	})
	if err != nil {
		t.Fatalf("разбор карты: %v", err)
	}
	want := map[string]int{"195.208.0.0/16": 12, "2a02:6b8::/32": 8, "192.0.2.0/24": 2}
	if len(overrides) != len(want) {
		t.Fatalf("ожидалось %d override, получено %v", len(want), overrides)
	}
	for _, o := range overrides {
		if o.Limit != want[o.Prefix.String()] {
			t.Errorf("override %s = %d, want %d", o.Prefix, o.Limit, want[o.Prefix.String()])
		}
	}

	for name, m := range map[string]map[string]int{
		"мусорный CIDR":  {"195.208.0.0.1/16": 8},
		"лимит меньше 1": {"195.208.0.0/16": 0},
		"дубль сети":     {"195.208.0.0/16": 8, "195.208.9.9/16": 12},
	} {
		if _, err := parsePerIPOverrides(m); err == nil {
			t.Errorf("%s: ожидалась ошибка разбора", name)
		}
	}
	if got, err := parsePerIPOverrides(nil); err != nil || got != nil {
		t.Errorf("пустая карта: %v, %v — ожидалось nil, nil", got, err)
	}
}

// TestLimitIPMapLimitFor: эффективный лимит IP — самый длинный совпавший
// префикс (longest-prefix, независимо от порядка карты), вне префиксов —
// общий; IPv4-в-IPv6 размапливается, семейства не пересекаются.
func TestLimitIPMapLimitFor(t *testing.T) {
	overrides, err := parsePerIPOverrides(map[string]int{
		"195.208.0.0/16": 5,
		"195.208.7.0/24": 3,
		"2a02:6b8::/32":  4,
	})
	if err != nil {
		t.Fatalf("разбор: %v", err)
	}
	st := newLimitState(LimitOptions{
		PerIPConns: 2, PerIPOverrides: overrides,
		MaxBodyBytes: 1024, MinBodySpeed: 1024, MinSpeedWindow: time.Second,
	})

	for _, tc := range []struct {
		ip   string
		want int
	}{
		{"195.208.7.1", 3},        // /24 длиннее /16
		{"195.208.9.9", 5},        // /16
		{"195.209.0.1", 2},        // вне префиксов — общий
		{"2a02:6b8::1", 4},        // IPv6-префикс
		{"2001:db8::1", 2},        // IPv6 вне
		{"::ffff:195.208.7.1", 3}, // IPv4-в-IPv6 — Unmap, семантически IPv4
	} {
		if got := st.ips.limitFor(netip.MustParseAddr(tc.ip)); got != tc.want {
			t.Errorf("limitFor(%s) = %d, want %d", tc.ip, got, tc.want)
		}
	}
}

// TestLimitIPMapOverrideCapacity: семафор IP внутри префикса держит лимит
// префикса (пятое соединение ждёт), IP вне — общий лимит (третье ждёт).
func TestLimitIPMapOverrideCapacity(t *testing.T) {
	st := newLimitState(LimitOptions{
		PerIPConns: 2, GlobalConns: 64, MaxRPS: 1000, Burst: 64,
		PerIPOverrides: []PerIPOverride{{Prefix: netip.MustParsePrefix("195.208.0.0/16"), Limit: 4}},
		MaxBodyBytes:   1024, MinBodySpeed: 1024, MinSpeedWindow: time.Second,
	})
	ctx := context.Background()

	// IP парковщика: 4 слота лимита префикса.
	park := netip.MustParseAddr("195.208.0.1")
	parkRel := make([]func(), 0, 4)
	for i := 0; i < 4; i++ {
		r, err := st.ips.acquire(ctx, park)
		if err != nil {
			t.Fatalf("слот %d в префиксе: %v", i+1, err)
		}
		parkRel = append(parkRel, r)
	}
	parkQueued := make(chan func(), 1)
	go func() {
		rel, err := st.ips.acquire(ctx, park)
		if err != nil {
			t.Errorf("пятое соединение префикса: %v", err)
			rel = func() {}
		}
		parkQueued <- rel
	}()
	select {
	case <-parkQueued:
		t.Fatal("пятый слот префикса взят при лимите 4")
	case <-time.After(100 * time.Millisecond):
	}

	// Обычный IP: общий лимит 2.
	plain := netip.MustParseAddr("192.0.2.7")
	r1, err := st.ips.acquire(ctx, plain)
	if err != nil {
		t.Fatalf("слот 1 вне префикса: %v", err)
	}
	r2, err := st.ips.acquire(ctx, plain)
	if err != nil {
		t.Fatalf("слот 2 вне префикса: %v", err)
	}
	plainQueued := make(chan func(), 1)
	go func() {
		rel, err := st.ips.acquire(ctx, plain)
		if err != nil {
			t.Errorf("третье соединение вне префикса: %v", err)
			rel = func() {}
		}
		plainQueued <- rel
	}()
	select {
	case <-plainQueued:
		t.Fatal("третий слот вне префикса взят при общем лимите 2")
	case <-time.After(100 * time.Millisecond):
	}

	// Освобождение будит соответствующую очередь (перекрёстно молчат).
	parkRel[0]()
	select {
	case rel := <-parkQueued:
		rel()
	case <-time.After(time.Second):
		t.Fatal("очередь префикса не пробудилась")
	}
	r1()
	select {
	case rel := <-plainQueued:
		rel()
	case <-time.After(time.Second):
		t.Fatal("очередь вне префикса не пробудилась")
	}
	for _, r := range parkRel[1:] {
		r()
	}
	r2()
}

// TestLimitIPMapWaitObserved: ожидание слота наблюдается onWait после
// выдачи (мгновенный слот — тоже, ≈0), отменённое ожидание наблюдением
// не становится; ждущий под держателем получает ненулевое ожидание.
func TestLimitIPMapWaitObserved(t *testing.T) {
	var mu sync.Mutex
	waits := make([]time.Duration, 0, 2)
	m := limitIPMap{
		limit:   1,
		onWait:  func(d time.Duration) { mu.Lock(); waits = append(waits, d); mu.Unlock() },
		entries: make(map[netip.Addr]*limitIPEntry),
	}
	ip := netip.MustParseAddr("192.0.2.1")
	ctx := context.Background()

	r1, err := m.acquire(ctx, ip)
	if err != nil {
		t.Fatalf("слот 1: %v", err)
	}

	// Ждущий под держателем: не возвращается, пока слот занят.
	got := make(chan func(), 1)
	go func() {
		rel, err := m.acquire(ctx, ip)
		if err != nil {
			t.Errorf("ждущий: %v", err)
			rel = func() {}
		}
		got <- rel
	}()
	select {
	case <-got:
		t.Fatal("второй слот взят при полном потолке")
	case <-time.After(100 * time.Millisecond):
	}

	// Отменённое ожидание наблюдением не становится (классификацию
	// ошибки этапа делает потребитель).
	cctx, cancel := context.WithCancel(ctx)
	cancelled := make(chan error, 1)
	go func() {
		_, err := m.acquire(cctx, ip)
		cancelled <- err
	}()
	cancel()
	select {
	case err := <-cancelled:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("отмена очереди: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("отмена не разбудила ждущего")
	}
	mu.Lock()
	n := len(waits)
	mu.Unlock()
	if n != 1 {
		t.Fatalf("после отмены наблюдений %d, want 1 (только мгновенный слот)", n)
	}

	// Выдача будится освобождением; ожидание очереди наблюдено.
	r1()
	select {
	case rel := <-got:
		rel()
	case <-time.After(time.Second):
		t.Fatal("очередь не пробудилась освобождением")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(waits) != 2 {
		t.Fatalf("наблюдений %d, want 2", len(waits))
	}
	if waits[1] < 20*time.Millisecond {
		t.Errorf("ожидание очереди %s, want ≥20ms (ждущий стоял ≥100ms)", waits[1])
	}
	if waits[0] >= waits[1] {
		t.Errorf("мгновенный слот %s не меньше ожидания очереди %s", waits[0], waits[1])
	}
}

// holdServer — сервер, обработчики которого живут до release: занятые
// соединения держат слоты лимитов, число вошедших видно вызывающему.
func holdServer(t *testing.T) (srv *httptest.Server, entered func() int64, release func()) {
	t.Helper()
	var n atomic.Int64
	ch := make(chan struct{})
	var once sync.Once
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		select {
		case <-ch:
		case <-time.After(10 * time.Second): // страховка от вечного зависания
		}
		_, _ = fmt.Fprint(w, "done")
	}))
	return srv, n.Load, func() { once.Do(func() { close(ch) }) }
}

// waitCount ждёт значения счётчика (диалог с тестовым сервером).
func waitCount(t *testing.T, entered func() int64, want int64) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if entered() == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("счётчик вошедших %d, ожидался %d", entered(), want)
}

// smallLimits — лимиты для интеграционных тестов: потолки соединений
// меньше числа запросов, темп не мешает.
func smallLimits(perIP, global int) *LimitOptions {
	return &LimitOptions{
		PerIPConns:     perIP,
		GlobalConns:    global,
		MaxRPS:         10000,
		MaxBodyBytes:   1 << 20,
		MinBodySpeed:   1 << 20,
		MinSpeedWindow: 100 * time.Millisecond,
	}
}

// TestFetcherPerIPLimitQueue: два соединения к одному IP держат слоты,
// третий запрос ждёт очереди per-IP и проходит после освобождения.
func TestFetcherPerIPLimitQueue(t *testing.T) {
	srv, entered, release := holdServer(t)
	defer srv.Close() // LIFO: release ниже закроет обработчики раньше
	defer release()

	f := NewFetcher(Options{
		UserAgent:     "t",
		HeaderTimeout: 5 * time.Second,
		TotalTimeout:  10 * time.Second,
		Limits:        smallLimits(2, 8),
	})
	u := hostURL(srv.URL, "perip.example.net")
	do := func(ctx context.Context) (*FetchResult, error) {
		return f.Fetch(ctx, FetchRequest{URL: u, Addrs: loopbackAddr()})
	}

	res1, err := do(context.Background())
	if err != nil {
		t.Fatalf("Fetch 1: %v", err)
	}
	res2, err := do(context.Background())
	if err != nil {
		t.Fatalf("Fetch 2: %v", err)
	}
	defer func() { _ = res2.Body.Close() }()
	waitCount(t, entered, 2)

	type result struct {
		res *FetchResult
		err error
	}
	third := make(chan result, 1)
	go func() {
		res, err := do(context.Background())
		third <- result{res, err}
	}()

	// Слоты заняты: третий не диалится — обработчиков по-прежнему два.
	time.Sleep(300 * time.Millisecond)
	if got := entered(); got != 2 {
		t.Fatalf("лимит per-IP пропущен: вошло обработчиков %d (ожидалось 2)", got)
	}

	// Освобождение: тела дочитываются, соединения закрываются, слоты
	// уходят ждущему третьему запросу.
	release()
	if _, err := io.ReadAll(res1.Body); err != nil {
		t.Fatalf("тело 1: %v", err)
	}
	_ = res1.Body.Close()
	if _, err := io.ReadAll(res2.Body); err != nil {
		t.Fatalf("тело 2: %v", err)
	}

	select {
	case got := <-third:
		if got.err != nil {
			t.Fatalf("Fetch 3 после освобождения: %v", got.err)
		}
		defer func() { _ = got.res.Body.Close() }()
		if got.res.StatusCode != http.StatusOK {
			t.Errorf("статус 3: %d", got.res.StatusCode)
		}
		if _, err := io.ReadAll(got.res.Body); err != nil {
			t.Errorf("тело 3: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("третий запрос не прошёл после освобождения слотов")
	}
	waitCount(t, entered, 3)
}

// TestFetcherPerIPQueueCancelled: ждущий в очереди per-IP запрос
// прерывается отменой ctx (таймаут вызывающего).
func TestFetcherPerIPQueueCancelled(t *testing.T) {
	srv, entered, release := holdServer(t)
	defer srv.Close()
	defer release()

	f := NewFetcher(Options{
		UserAgent:     "t",
		HeaderTimeout: 5 * time.Second,
		TotalTimeout:  10 * time.Second,
		Limits:        smallLimits(2, 8),
	})
	u := hostURL(srv.URL, "cancel-queue.example.net")
	fetch := func() (*FetchResult, error) {
		return f.Fetch(context.Background(), FetchRequest{URL: u, Addrs: loopbackAddr()})
	}

	res1, err := fetch()
	if err != nil {
		t.Fatalf("Fetch 1: %v", err)
	}
	defer func() { _ = res1.Body.Close() }()
	res2, err := fetch()
	if err != nil {
		t.Fatalf("Fetch 2: %v", err)
	}
	defer func() { _ = res2.Body.Close() }()
	waitCount(t, entered, 2)

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = f.Fetch(ctx, FetchRequest{URL: u, Addrs: loopbackAddr()})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("ожидалась ошибка отмены запроса в очереди per-IP")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("ошибка очереди не пробрасывает DeadlineExceeded: %v", err)
	}
	if elapsed >= 3*time.Second {
		t.Errorf("отмена очереди сработала поздно: %s", elapsed)
	}
}

// --- потолок тела --------------------------------------------------------------

// TestFetchBodyLimitExactAndTruncated: тело ровно в лимит читается
// целиком без признака обрезки; сверх лимита — обрезается с признаком
// Truncated (не ошибка сети), включая ответы без Content-Length.
func TestFetchBodyLimitExactAndTruncated(t *testing.T) {
	body := func(size int, contentLength bool) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if contentLength {
				w.Header().Set("Content-Length", fmt.Sprint(size))
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(bytes.Repeat([]byte("x"), size))
			if !contentLength {
				if f, ok := w.(http.Flusher); ok {
					f.Flush()
				}
			}
		}
	}

	run := func(t *testing.T, h http.HandlerFunc, size, limit int, wantTrunc bool) {
		t.Helper()
		srv := httptest.NewServer(h)
		defer srv.Close()
		f := NewFetcher(Options{
			UserAgent:    "t",
			TotalTimeout: 5 * time.Second,
			Limits: &LimitOptions{
				PerIPConns: 4, GlobalConns: 8, MaxRPS: 10000,
				MaxBodyBytes: int64(limit), MinBodySpeed: 1 << 20, MinSpeedWindow: 100 * time.Millisecond,
			},
		})
		res, err := f.Fetch(context.Background(), FetchRequest{URL: hostURL(srv.URL, "body.example.net"), Addrs: loopbackAddr()})
		if err != nil {
			t.Fatalf("Fetch: %v", err)
		}
		got, readErr := io.ReadAll(res.Body)
		_ = res.Body.Close()
		if readErr != nil {
			t.Fatalf("обрезка — не ошибка чтения: %v", readErr)
		}
		if len(got) != min(size, limit) {
			t.Errorf("прочитано %d байт, ожидалось %d", len(got), min(size, limit))
		}
		if res.BodyLimiter == nil {
			t.Fatal("BodyLimiter не подставлен")
		}
		if res.BodyLimiter.Truncated() != wantTrunc {
			t.Errorf("признак обрезки: %v, ожидался %v", res.BodyLimiter.Truncated(), wantTrunc)
		}
	}

	t.Run("ровно в лимит", func(t *testing.T) { run(t, body(1024, true), 1024, 1024, false) })
	t.Run("сверх лимита", func(t *testing.T) { run(t, body(4096, true), 4096, 1024, true) })
	t.Run("сверх лимита chunked", func(t *testing.T) { run(t, body(4096, false), 4096, 1024, true) })
}

// --- анти-тарпит ----------------------------------------------------------------

// TestFetchBodyTarpitSpeed: заголовки и первый кусок тела пришли, хвост
// молчит — дедлайн дочитания остатка (продлеваемый по мере чтения)
// истекает, чтение прерывается таймаут-классифицированной ошибкой,
// соединение закрывается.
func TestFetchBodyTarpitSpeed(t *testing.T) {
	srv, done := stallServer(t, func(w http.ResponseWriter, stall <-chan struct{}) {
		w.Header().Set("Content-Length", "8192")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(bytes.Repeat([]byte("x"), 1024))
		if f, ok := w.(http.Flusher); ok {
			f.Flush() // первый байт у клиента — дальше тарпит
		}
		select {
		case <-stall:
		case <-time.After(5 * time.Second):
		}
		_, _ = w.Write(bytes.Repeat([]byte("x"), 7168))
	})
	defer done()

	f := NewFetcher(Options{
		UserAgent:    "t",
		TotalTimeout: 10 * time.Second,
		Limits: &LimitOptions{
			PerIPConns: 4, GlobalConns: 16, MaxRPS: 10000,
			MaxBodyBytes: 8192, MinBodySpeed: 1 << 20, MinSpeedWindow: 200 * time.Millisecond,
		},
	})
	res, err := f.Fetch(context.Background(), FetchRequest{URL: hostURL(srv.URL, "tarpit-body.example.net"), Addrs: loopbackAddr()})
	if err != nil {
		t.Fatalf("заголовки должны успеть: %v", err)
	}
	start := time.Now()
	_, readErr := io.ReadAll(res.Body)
	elapsed := time.Since(start)
	if readErr == nil {
		t.Fatal("тарпит тела должен прерываться нарушением скорости")
	}
	if !IsTimeout(readErr) {
		t.Errorf("нарушение скорости — тайм-классификация, получено: %v", readErr)
	}
	if !strings.Contains(readErr.Error(), "тарпит") {
		t.Errorf("ошибка без указания причины: %v", readErr)
	}
	if elapsed < 150*time.Millisecond || elapsed >= 2*time.Second {
		t.Errorf("нарушение скорости сработало вне границ окна: %s", elapsed)
	}
	// Соединение закрыто: повторное чтение — та же ошибка, не блокировка.
	if _, err := res.Body.Read(make([]byte, 16)); err == nil || !IsTimeout(err) {
		t.Errorf("чтение после нарушения: %v", err)
	}
	_ = res.Body.Close()
}

// TestFetchBodySpeedPacedOK: тело капает быстрее минимальной скорости —
// продлеваемый дедлайн не срабатывает, тело читается целиком.
func TestFetchBodySpeedPacedOK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "4096")
		w.WriteHeader(http.StatusOK)
		for i := 0; i < 4; i++ {
			_, _ = w.Write(bytes.Repeat([]byte("x"), 1024))
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			time.Sleep(60 * time.Millisecond) // ~17 КиБ/с > минимума 4 КиБ/с
		}
	}))
	defer srv.Close()

	f := NewFetcher(Options{
		UserAgent:    "t",
		TotalTimeout: 10 * time.Second,
		Limits: &LimitOptions{
			PerIPConns: 4, GlobalConns: 16, MaxRPS: 10000,
			MaxBodyBytes: 4096, MinBodySpeed: 4096, MinSpeedWindow: 150 * time.Millisecond,
		},
	})
	res, err := f.Fetch(context.Background(), FetchRequest{URL: hostURL(srv.URL, "paced.example.net"), Addrs: loopbackAddr()})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	got, readErr := io.ReadAll(res.Body)
	_ = res.Body.Close()
	if readErr != nil {
		t.Fatalf("ложное срабатывание анти-тарпита: %v", readErr)
	}
	if len(got) != 4096 {
		t.Errorf("прочитано %d байт, ожидалось 4096", len(got))
	}
	if res.BodyLimiter.Truncated() {
		t.Error("тело в лимите не должно получать признак обрезки")
	}
}

// --- темп запросов и keep-alive ---------------------------------------------------

// TestRoundTripRPSPacing: токен-ведро темпа в RoundTrip — запросы сверх
// всплеска выдерживают интервал темпа.
func TestRoundTripRPSPacing(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, "ok")
	}))
	defer srv.Close()

	f := NewFetcher(Options{
		UserAgent: "t",
		Limits: &LimitOptions{
			PerIPConns: 4, GlobalConns: 8, MaxRPS: 20, Burst: 1,
			MaxBodyBytes: 1 << 20, MinBodySpeed: 1 << 20, MinSpeedWindow: 100 * time.Millisecond,
		},
	})
	start := time.Now()
	for i := 0; i < 3; i++ {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL, nil)
		if err != nil {
			t.Fatalf("запрос: %v", err)
		}
		resp, err := f.RoundTrip(req)
		if err != nil {
			t.Fatalf("RoundTrip #%d: %v", i+1, err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
	elapsed := time.Since(start)
	// Всплеск 1 + 2 токена по 50 мс ≈ 100 мс.
	if elapsed < 90*time.Millisecond {
		t.Errorf("темп RPS не выдержан: 3 запроса за %s (ожидалось ≥90ms)", elapsed)
	}
	if elapsed >= 2*time.Second {
		t.Errorf("темп RPS затянут: %s", elapsed)
	}
}

// TestKeepAliveConnectionNotReused: keep-alive отключён (ТЗ 5.5: «отключён
// или очень короткий idle»; выбор T-047/T-049) — два последовательных
// запроса к одному хосту идут по разным соединениям, клиент шлёт
// Connection: close.
func TestKeepAliveConnectionNotReused(t *testing.T) {
	var mu sync.Mutex
	remotes := make(map[string]bool)
	closeHeader := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		remotes[r.RemoteAddr] = true
		if r.Close {
			closeHeader++
		}
		mu.Unlock()
		_, _ = fmt.Fprint(w, "ok")
	}))
	defer srv.Close()

	f := NewFetcher(Options{UserAgent: "t"})
	for i := 0; i < 2; i++ {
		res, err := f.Fetch(context.Background(), FetchRequest{URL: hostURL(srv.URL, "keepalive.example.net"), Addrs: loopbackAddr()})
		if err != nil {
			t.Fatalf("Fetch #%d: %v", i+1, err)
		}
		if _, err := io.ReadAll(res.Body); err != nil {
			t.Fatalf("тело #%d: %v", i+1, err)
		}
		_ = res.Body.Close()
	}
	mu.Lock()
	defer mu.Unlock()
	if len(remotes) != 2 {
		t.Errorf("ожидались 2 разных соединения (keep-alive выключен), соединений: %d", len(remotes))
	}
	if closeHeader != 2 {
		t.Errorf("Connection: close в обоих запросах: заголовок в %d из 2", closeHeader)
	}
}

// TestFetchWithoutLimitsNoLimiter: без Options.Limits поведение T-047
// сохраняется — BodyLimiter nil, тело без обёртки.
func TestFetchWithoutLimitsNoLimiter(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, "plain")
	}))
	defer srv.Close()

	f := NewFetcher(Options{UserAgent: "t"})
	res, err := f.Fetch(context.Background(), FetchRequest{URL: hostURL(srv.URL, "plain.example.net"), Addrs: loopbackAddr()})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	defer func() { _ = res.Body.Close() }()
	if res.BodyLimiter != nil {
		t.Error("без Options.Limits BodyLimiter должен быть nil")
	}
	body, err := io.ReadAll(res.Body)
	if err != nil || string(body) != "plain" {
		t.Errorf("тело без лимитов: %q, %v", body, err)
	}
}

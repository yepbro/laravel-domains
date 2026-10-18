// Лимиты и защита HTTP-этапа (ТЗ 5.5, таблица лимитов; ТЗ 8: «тарпиты,
// бесконечные редиректы, огромные ответы, zip-бомбы — прерываются по
// лимитам из 5.5»). Пять механизмов:
//
//   - потолок тела MaxBodyBytes («макс. размер читаемого тела | 2 МБ»):
//     превышение — не ошибка сети: тело обрезается, признак «данные
//     частичны» выставляется в BodyLimiter.Truncated — решение за
//     потребителем (T-058), транспорт лишь сигналит;
//   - анти-тарпит («мин. скорость 2 КБ/с после первого байта»): сторож
//     с продлеваемым дедлайном — перед каждым чтением тела дедлайн
//     дочитывания остатка пересчитывается как now + remain/speed;
//     инвариант: суммарное время чтения не превосходит начальный
//     остаток/скорость, то есть средняя скорость от любого момента до
//     конца — не ниже минимальной. Нарушение — таймаут-классификация
//     (limitBodySpeedError → net.Error) и закрытие соединения отменой
//     контекста запроса; дедлайн не короче MinSpeedWindow — пол
//     закрывает джиттер планировщика и RTT последнего сегмента;
//   - соединения на один IP («одновременных соединений на один IP |
//     3–4»): семафор-карта по диалимому IP (адрес DNS-этапа или резолва
//     редиректа, не хост URL) — limitIPMap; ожидающие стоят в очереди
//     с отменой ctx;
//   - глобальное число соединений и темп запросов («ограничение
//     исходящей нагрузки по глобальному числу соединений и RPS»):
//     счётный семафор на всё соединение и токен-ведро на запрос —
//     limitBucket, механика и обоснование как у dnsclient.limitBucket
//     (единый стиль пакетов лимитов): средний темп max_rps, короткий
//     всплеск до burst — «пачка уходит сразу, темп выравнивается»;
//     фиксированное окно резало бы естественные всплески конвейера.
//
// Интеграция — через Options.Limits (nil — транспорт без лимитов:
// изолированные тесты слоёв T-047; NewOptions включает лимиты из
// секции http конфигурации всегда). Слоты соединений занимаются в
// dialLimited до старта connect-таймаута: ожидание в очереди — не
// часть установки соединения; высвобождаются закрытием соединения
// (limitConn). Keep-alive остаётся отключённым (выбор T-047 из
// «отключён или очень короткий idle» ТЗ 5.5, зафиксирован в T-049:
// одна главная страница на сайт, пул по хосту переживал бы перерезолв
// и держал тысячи неактивных соединений при concurrency 2000).

package httpfetch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/netip"
	"sync"
	"time"

	"crawler/internal/config"
)

// Значения по умолчанию — таблица лимитов ТЗ 5.5 («все настраиваются»)
// и ТЗ 5.10. Числа без ТЗ (RPS, окно скорости) — дефолты выведены из
// свойств этапа, переопределяются в LimitOptions и конфигурации.
const (
	// DefaultLimitBodyBytes — потолок читаемого тела («макс. размер
	// читаемого тела | 2 МБ», ТЗ 5.5; http.max_body_bytes).
	DefaultLimitBodyBytes = int64(2 << 20)
	// DefaultLimitBodySpeed — мин. скорость чтения тела после первого
	// байта («защита от тарпитов | мин. скорость 2 КБ/с», ТЗ 5.5;
	// http.min_body_speed), байт/с.
	DefaultLimitBodySpeed = int64(2 << 10)
	// DefaultLimitSpeedWindow — пол дедлайна анти-тарпита: дедлайн
	// дочитания остатка не короче этого окна. Джиттер планирования
	// горутин и сетевой RTT последнего сегмента не должны давать ложных
	// срабатываний у серверов, держащих скорость; на защиту пол не
	// влияет: остановка тела ловится окном вместо нулевого дедлайна.
	DefaultLimitSpeedWindow = 500 * time.Millisecond
	// DefaultLimitPerIPConns — одновременных соединений на один IP
	// (рекомендация ТЗ 5.5 — 3–4; http.per_ip_limit): верх диапазона.
	DefaultLimitPerIPConns = 4
	// DefaultLimitGlobalConns — глобальный потолок соединений
	// (http.concurrency, ТЗ 5.10): счётчик соединений — та же величина,
	// что плановый параллелизм воркеров.
	DefaultLimitGlobalConns = 2000
	// DefaultLimitRPS — средний темп запросов (http.max_rps). ТЗ числа
	// не задаёт: страница занимает ~1–2 с, при concurrency 2000
	// естественный темп ~1000–2000 запросов/с — дефолт 1000 держит
	// порядок исходящей нагрузки одного IP (тюнинг сети под неё —
	// ТЗ 10.2), не резать ниже — настройкой.
	DefaultLimitRPS = 1000
)

// LimitOptions — параметры лимитов T-049. Нулевые значения заменяются
// дефолтами (withDefaults, стиль Options: «0 — значение по умолчанию»).
type LimitOptions struct {
	// MaxBodyBytes — потолок читаемого тела (http.max_body_bytes).
	// Превышение — обрезка и признак BodyLimiter.Truncated, не ошибка
	// сети (ТЗ 8: решение за потребителем T-058).
	MaxBodyBytes int64
	// MinBodySpeed — мин. скорость чтения тела после первого байта,
	// байт/с (http.min_body_speed). Нарушение — таймаут, соединение
	// закрывается.
	MinBodySpeed int64
	// MinSpeedWindow — пол дедлайна анти-тарпита (см.
	// DefaultLimitSpeedWindow).
	MinSpeedWindow time.Duration
	// PerIPConns — одновременных соединений на один диалимый IP
	// (http.per_ip_limit).
	PerIPConns int
	// GlobalConns — глобальный потолок одновременных соединений
	// (http.concurrency).
	GlobalConns int
	// MaxRPS — средний темп запросов, токен-ведро (http.max_rps).
	MaxRPS int
	// Burst — ёмкость токен-ведра: запросов подряд без ожидания
	// (короткий всплеск конвейера). По умолчанию GlobalConns — «полная
	// волна» всех воркеров разом; больше бессмысленно: одновременно
	// летит не больше соединений.
	Burst int
}

// NewLimitOptions собирает лимиты из секции http: потолок тела и
// per-IP есть с T-006, скорость тела и RPS — поля T-049; глобальный
// потолок соединений — http.concurrency.
func NewLimitOptions(cfg config.HTTP) LimitOptions {
	return LimitOptions{
		MaxBodyBytes: cfg.MaxBodyBytes.B(),
		MinBodySpeed: cfg.MinBodySpeed.B(),
		PerIPConns:   cfg.PerIPLimit,
		GlobalConns:  cfg.Concurrency,
		MaxRPS:       cfg.MaxRPS,
	}
}

// withDefaults заменяет нулевые и отрицательные значения дефолтами.
func (o LimitOptions) withDefaults() LimitOptions {
	if o.MaxBodyBytes <= 0 {
		o.MaxBodyBytes = DefaultLimitBodyBytes
	}
	if o.MinBodySpeed <= 0 {
		o.MinBodySpeed = DefaultLimitBodySpeed
	}
	if o.MinSpeedWindow <= 0 {
		o.MinSpeedWindow = DefaultLimitSpeedWindow
	}
	if o.PerIPConns <= 0 {
		o.PerIPConns = DefaultLimitPerIPConns
	}
	if o.GlobalConns <= 0 {
		o.GlobalConns = DefaultLimitGlobalConns
	}
	if o.MaxRPS <= 0 {
		o.MaxRPS = DefaultLimitRPS
	}
	if o.Burst <= 0 {
		o.Burst = o.GlobalConns
	}
	return o
}

// limitState — активные лимиты одного Fetcher: глобальный семафор
// соединений, токен-ведро темпа и семафоры по IP. Потокобезопасно,
// переживает все запросы транспорта.
type limitState struct {
	opt    LimitOptions
	global chan struct{}
	rps    *limitBucket
	ips    limitIPMap
}

// newLimitState строит состояние из опций (вызывающий уже подставил
// дефолты).
func newLimitState(opt LimitOptions) *limitState {
	return &limitState{
		opt:    opt,
		global: make(chan struct{}, opt.GlobalConns),
		rps:    newLimitBucket(float64(opt.MaxRPS), opt.Burst),
		ips: limitIPMap{
			limit:   opt.PerIPConns,
			entries: make(map[netip.Addr]*limitIPEntry),
		},
	}
}

// acquireDial занимает слоты одного соединения к addr: сперва per-IP
// (узкий ресурс — их 3–4), затем глобальный — порядок исключает
// простой глобального слота в ожидании узкого. Очередь ждёт
// освобождения или отмены ctx; отменившийся слотов не занимает.
func (l *limitState) acquireDial(ctx context.Context, addr netip.Addr) (func(), error) {
	releaseIP, err := l.ips.acquire(ctx, addr)
	if err != nil {
		return nil, err
	}
	if err := limitChanAcquire(ctx, l.global); err != nil {
		releaseIP()
		return nil, err
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			limitChanRelease(l.global)
			releaseIP()
		})
	}, nil
}

// limitChanAcquire занимает слот семафора-канала; ожидание прерывается
// отменой ctx. Канал будит ждущих в порядке поступления (FIFO) —
// очередь справедлива без дополнительного кода.
func limitChanAcquire(ctx context.Context, ch chan struct{}) error {
	select {
	case ch <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// limitChanRelease возвращает слот семафора-канала.
func limitChanRelease(ch chan struct{}) { <-ch }

// limitIPMap — семафоры «соединений на один IP» (ТЗ 5.5): запись на IP
// создаётся по требованию и удаляется, когда её никто не держит и
// никто не ждёт (refs) — карта не растёт с числом проверенных IP,
// размер ограничен активностью (конкурентностью) транспорта.
type limitIPMap struct {
	limit   int
	mu      sync.Mutex
	entries map[netip.Addr]*limitIPEntry
}

// limitIPEntry — семафор одного IP и счётчик интересантов (держащих
// слот и стоящих в очереди): каждый acquire увеличивает refs, отпускание
// слота или отмена ожидания уменьшают; по нулю запись исчезает из карты.
type limitIPEntry struct {
	sem  chan struct{}
	refs int
}

// acquire занимает слот соединения с IP; при исчерпании — очередь до
// освобождения или отмены ctx. Возвращаемая функция потокобезопасна и
// идемпотентна (повторный вызов ничего не портит).
func (m *limitIPMap) acquire(ctx context.Context, ip netip.Addr) (func(), error) {
	m.mu.Lock()
	e, ok := m.entries[ip]
	if !ok {
		e = &limitIPEntry{sem: make(chan struct{}, m.limit)}
		m.entries[ip] = e
	}
	e.refs++
	m.mu.Unlock()

	select {
	case e.sem <- struct{}{}:
		var once sync.Once
		return func() {
			once.Do(func() {
				<-e.sem
				m.drop(ip, e)
			})
		}, nil
	case <-ctx.Done():
		m.drop(ip, e)
		return nil, ctx.Err()
	}
}

// drop уменьшает счётчик интересантов записи и удаляет опустевшую из
// карты. Удаление по тождеству записи: между нулем refs и удалением
// по тому же IP могла быть создана новая запись.
func (m *limitIPMap) drop(ip netip.Addr, e *limitIPEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e.refs--
	if e.refs == 0 && m.entries[ip] == e {
		delete(m.entries, ip)
	}
}

// limitBucket — токен-ведро темпа запросов с ленивым пополнением:
// токены начисляются в момент обращения за (now−last)×rate, без
// фоновых горутин. Выбор против фиксированного окна «N в секунду» —
// как у dnsclient.limitBucket (единый стиль): ведро держит средний
// темп и допускает короткий всплеск до burst, окно резало бы пачки
// субдоменов и добавляло джиттер на границах.
type limitBucket struct {
	mu     sync.Mutex
	rate   float64 // токенов в секунду (эффективный RPS)
	burst  float64 // ёмкость ведра
	tokens float64
	last   time.Time // момент последнего начисления
}

// newLimitBucket создаёт полное ведро (burst токенов): плановая волна
// старта конвейера уходит без ожидания.
func newLimitBucket(rps float64, burst int) *limitBucket {
	return &limitBucket{
		rate:   rps,
		burst:  float64(burst),
		tokens: float64(burst),
		last:   time.Now(),
	}
}

// take забирает токен; при пустом ведре ждёт начисления следующего или
// отмены ctx (ожидающий не занимает токен навсегда).
func (b *limitBucket) take(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		wait, ok := b.tryTake()
		if ok {
			return nil
		}
		if err := limitSleep(ctx, wait); err != nil {
			return err
		}
	}
}

// tryTake забирает токен без ожидания; при нехватке возвращает время
// до следующего токена.
func (b *limitBucket) tryTake() (time.Duration, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill()
	if b.tokens >= 1 {
		b.tokens--
		return 0, true
	}
	// (1 − tokens)/rate секунд до целого токена; минимум 1 нс — защита
	// от нулевого ожидания на округлении (иначе take крутился бы
	// вхолостую).
	wait := time.Duration((1 - b.tokens) / b.rate * float64(time.Second))
	if wait < 1 {
		wait = 1
	}
	return wait, false
}

// refill начисляет токены по прошедшему времени; вызывающий держит b.mu.
func (b *limitBucket) refill() {
	if now := time.Now(); now.After(b.last) {
		b.tokens = math.Min(b.burst, b.tokens+now.Sub(b.last).Seconds()*b.rate)
		b.last = now
	}
}

// limitSleep — пауза, прерываемая отменой ctx.
func limitSleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// limitConn — соединение со слотами лимитов (per-IP и глобальный):
// слоты живут ровно пока живо соединение, Close их возвращает. Слоты
// у одного соединения — единственный владелец, повторный Close
// (транспорт может звать) не высвобождает дважды.
type limitConn struct {
	net.Conn
	release func()
	once    sync.Once
}

func (c *limitConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.release)
	return err
}

// dialLimited — попытка подключения под лимитами соединений T-049:
// слоты per-IP и глобальный занимаются до старта connect-таймаута
// (ожидание в очереди — не часть установки соединения), успешное
// соединение оборачивается в limitConn. Без Options.Limits — прямой
// проход к dialTimed (транспорт без лимитов, T-047).
func (f *Fetcher) dialLimited(ctx context.Context, addr netip.Addr, port string) (net.Conn, error) {
	if f.lim == nil {
		return f.dialTimed(ctx, addr, port)
	}
	release, err := f.lim.acquireDial(ctx, addr)
	if err != nil {
		return nil, err
	}
	conn, err := f.dialTimed(ctx, addr, port)
	if err != nil {
		release()
		return nil, err
	}
	return &limitConn{Conn: conn, release: release}, nil
}

// errLimitBodyClosed — чтение после закрытия тела.
var errLimitBodyClosed = errors.New("httpfetch: чтение закрытого тела")

// BodyLimiter — тело ответа под лимитами T-049: потолок MaxBodyBytes
// (обрезка с признаком Truncated, не ошибка сети) и анти-тарпит
// (мин. скорость после первого байта). Реализует io.ReadCloser; в
// FetchResult дублируется полем BodyLimiter — признак обрезки виден
// потребителю (T-058) без привязки к фактическому типу Body.
//
// Анти-тарпит: перед каждым чтением взводится сторож с дедлайном
// дочитания остатка now + remain/speed (продлевается по мере чтения —
// инвариант «средняя скорость до конца не ниже минимальной»);
// нарушение — отмена контекста запроса: соединение закрывается,
// блокированное чтение возвращается, Read отдаёт limitBodySpeedError
// (тайм-классификация по IsTimeout). Сторож — time.AfterFunc без
// горутин ожидания: снятие чтением, утечек нет.
type BodyLimiter struct {
	rc     io.ReadCloser
	cancel context.CancelFunc

	speed    int64         // байт/с; 0 — анти-тарпит выключен
	window   time.Duration // пол дедлайна (MinSpeedWindow)
	remain   int64         // осталось до потолка MaxBodyBytes
	trunc    bool          // тело длиннее потолка — данные частичны
	probed   bool          // хвост за потолком уже классифицирован
	probeErr error         // итог классификации хвоста (повторные чтения источник не трогают)

	mu     sync.Mutex
	timer  *time.Timer
	fired  bool // сторож сработал — нарушение скорости
	closed bool
}

// newBodyLimiter оборачивает rc (тело с общей отменой запроса —
// cancelBody); cancel закрывает соединение при нарушении скорости.
func newBodyLimiter(rc io.ReadCloser, cancel context.CancelFunc, opt LimitOptions) *BodyLimiter {
	return &BodyLimiter{
		rc:     rc,
		cancel: cancel,
		speed:  opt.MinBodySpeed,
		window: opt.MinSpeedWindow,
		remain: opt.MaxBodyBytes,
	}
}

// Read читает тело не дальше потолка MaxBodyBytes. Когда потолок отдан,
// единственный пробный байт решает, длиннее ли тело лимита: байт есть —
// truncated=true и io.EOF (обрезка, не ошибка), сразу io.EOF — тело
// ровно в лимите; итог пробы запоминается — повторные чтения возвращают
// его, не трогая источник. Буфер длины 0 не читает источник вовсе
// (контракт io.Reader). Проба идёт под тем же сторожем скорости:
// молчащий хвост — то же нарушение темпа.
func (b *BodyLimiter) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return 0, errLimitBodyClosed
	}
	if b.fired {
		b.mu.Unlock()
		return 0, b.speedError()
	}
	if b.remain <= 0 {
		if b.probed {
			probeErr := b.probeErr
			b.mu.Unlock()
			return 0, probeErr
		}
		b.mu.Unlock()
		return b.probeTail()
	}
	if int64(len(p)) > b.remain {
		p = p[:int(b.remain)] // за потолок не читаем
	}
	b.armLocked()
	b.mu.Unlock()

	n, err := b.rc.Read(p)

	b.mu.Lock()
	b.disarmLocked()
	fired := b.fired
	if n > 0 {
		b.remain -= int64(n)
	}
	b.mu.Unlock()

	if fired {
		return n, b.speedError()
	}
	return n, err
}

// probeTail — классификация хвоста после исчерпания потолка (см. Read);
// выполняется один раз, итог запоминается (probed/probeErr).
func (b *BodyLimiter) probeTail() (int, error) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return 0, errLimitBodyClosed
	}
	b.armLocked() // remain = 0: дедлайн — только пол окна
	b.mu.Unlock()

	var one [1]byte
	n, err := b.rc.Read(one[:])

	b.mu.Lock()
	b.disarmLocked()
	fired := b.fired
	b.probed = true
	if n > 0 {
		b.trunc = true
		b.probeErr = io.EOF // классифицировано как обрезка
	} else {
		b.probeErr = err // io.EOF — ровно в лимите; ошибка сети — как есть
	}
	b.mu.Unlock()

	if fired {
		return 0, b.speedError()
	}
	if n > 0 {
		return 0, io.EOF // тело обрезано потолком
	}
	return 0, err // io.EOF — ровно в лимите; ошибка сети — как есть
}

// Close останавливает сторож и закрывает тело.
func (b *BodyLimiter) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	b.disarmLocked()
	b.mu.Unlock()
	return b.rc.Close()
}

// Truncated — тело превысило потолок MaxBodyBytes и обрезано: данные
// частичны, не ошибка сети (ТЗ 8). Признак выставляется чтением к
// моменту io.EOF от Body.
func (b *BodyLimiter) Truncated() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.trunc
}

// armLocked взводит сторож анти-тарпита: дедлайн дочитания остатка
// remain/speed от текущего момента (ТЗ 5.5: мин. скорость после
// первого байта). Вызывающий держит b.mu.
func (b *BodyLimiter) armLocked() {
	if b.speed <= 0 {
		return
	}
	d := b.deadlineLocked(b.remain)
	if b.timer == nil {
		b.timer = time.AfterFunc(d, b.onDeadline)
		return
	}
	b.timer.Reset(d)
}

// deadlineLocked — дедлайн дочитания остатка; не короче окна-пола
// (джиттер планировщика, RTT последнего сегмента).
func (b *BodyLimiter) deadlineLocked(remain int64) time.Duration {
	if b.speed <= 0 {
		return 0
	}
	d := time.Duration(float64(remain) / float64(b.speed) * float64(time.Second))
	if d < b.window {
		d = b.window
	}
	return d
}

// disarmLocked снимает сторож (чтение вернулось). Вызывающий держит b.mu.
func (b *BodyLimiter) disarmLocked() {
	if b.timer != nil {
		b.timer.Stop()
	}
}

// onDeadline — дедлайн истёк: скорость нарушена. Отмена контекста
// запроса закрывает соединение — блокированное чтение возвращается
// ошибкой, слоты лимитов соединений освобождаются (ТЗ 5.5: «нарушение
// — соединение закрывается»).
func (b *BodyLimiter) onDeadline() {
	b.mu.Lock()
	b.fired = true
	b.mu.Unlock()
	b.cancel()
}

// speedError — ошибка нарушения скорости с параметрами на момент.
func (b *BodyLimiter) speedError() error {
	b.mu.Lock()
	speed, remain := b.speed, b.remain
	b.mu.Unlock()
	return &limitBodySpeedError{speed: speed, remain: remain}
}

// limitBodySpeedError — нарушение мин. скорости чтения тела (тарпит,
// ТЗ 5.5). net.Error с Timeout()=true: классификация потребителя —
// таймаут (temp_error, как у прочих слоёв — IsTimeout), не «нет сайта».
type limitBodySpeedError struct {
	speed  int64 // настройка, байт/с
	remain int64 // байт оставалось дочитать
}

func (e *limitBodySpeedError) Error() string {
	return fmt.Sprintf("httpfetch: тело отдаётся медленнее %d байт/с (осталось %d байт) — тарпит", e.speed, e.remain)
}

func (e *limitBodySpeedError) Timeout() bool   { return true }
func (e *limitBodySpeedError) Temporary() bool { return true }

// compile-time check: *limitBodySpeedError — net.Error (IsTimeout).
var _ net.Error = (*limitBodySpeedError)(nil)

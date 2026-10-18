// Лимиты DNS-этапа (ТЗ 5.4: «Ограничение параллелизма и QPS настраивается;
// адаптивное снижение при росте доли таймаутов», «Если на сервере нет IPv6 —
// AAAA-запросы отключаются конфигом») — декоратор LimitResolver над
// интерфейсом Resolver — точкой расширения, оставленной T-034. Контракт
// Exchange не меняется: потребитель (T-036…T-039) получает те же Result и
// ошибки, что и без обёртки; RCODE по-прежнему не ошибка.
//
// Три механизма:
//
//   - параллелизм — семафор limitSemaphore с изменяемым потолком: адаптив
//     опускает потолок для новых обменов, не отбирая слоты у летящих
//     (снижение мягкое: очередь дренируется естественным темпом ответов);
//   - темп — токен-ведро limitBucket со средним темпом dns.max_qps и
//     короткими всплесками до burst (обоснование выбора — у limitBucket);
//   - адаптив — доля таймаутов (IsTimeout) в скользящем окне последних
//     обменов; выше порога оба лимита снижаются, ниже порога восстановления
//     растут (гистерезис; механика и направления — у limitAdaptive).
//
// Порядок ожиданий в Exchange: сначала семафор, затем токен. Токен берётся
// вплотную к отправке: возьми горутины токены заранее и выстройся в очередь
// слотов — накопленное ведро позволяло бы всплеск мимо сниженного QPS.
// Обратная цена (слот занят на время ожидания токена) не страшна: когда темп
// ограничен QPS, реальная параллельность обменов и так ниже потолка.
//
// Отключение AAAA (dns.ipv6=false) — локальный пустой NOERROR-ответ без
// похода к резолверу и без участия лимитов и окна адаптива: резолвер не
// нагружается, а доля таймаутов не разбавляется фиктивными успехами.
//
// Семафор — защита от перерасхода, а не очередь на тысячи горутин:
// потребитель T-036+ держит воркеров не больше concurrency, поэтому у
// limitSemaphore широковещательное пробуждение (редкие изменения состояния),
// а не хитрые схемы «разбудить одного».

package dnsclient

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/miekg/dns"

	"crawler/internal/config"
)

// Значения по умолчанию. Плановые лимиты — из ТЗ 5.10 (dns.concurrency 2000,
// dns.max_qps 10000); механика адаптива в ТЗ числами не задана — дефолты
// выведены из свойств этапа и переопределяются в LimitOptions.
const (
	// DefaultLimitConcurrency — плановый параллелизм DNS-этапа
	// (dns.concurrency, ТЗ 5.10). Совпадает с DefaultUDPPoolSize: пул
	// сокетов не должен резать плановый параллелизм (T-034).
	DefaultLimitConcurrency = 2000
	// DefaultLimitQPS — плановый темп обменов (dns.max_qps, ТЗ 5.10).
	DefaultLimitQPS = 10000
	// DefaultLimitWindow — окно доли таймаутов: 200 обменов. При плановых
	// 10000 QPS окно собирается за ~20 мс — быстрее одного цикла ретраев
	// (2 с × 3 попытки), при этом единичные таймауты не качают решение.
	DefaultLimitWindow = 200
	// DefaultLimitHighRatio — порог снижения: доля таймаутов ≥ 10 %.
	// У здорового рекурсивного резолвера с тёплым кэшем (ТЗ 5.4) таймауты —
	// доли процента; 10 % — однозначно аномалия.
	DefaultLimitHighRatio = 0.10
	// DefaultLimitLowRatio — порог восстановления: доля ≤ 2 % (гистерезис
	// с зоной нечувствительности 2…10 % — устойчивое «чуть плохо» не
	// раскачивает лимиты туда-сюда).
	DefaultLimitLowRatio = 0.02
	// DefaultLimitDownFactor — множитель снижения ×0.5: пока резолвер
	// таймаутит, каждый обмен жжёт бюджет попыток и держит слот — снижать
	// надо решительно (мультипликативно, как AIMD-уменьшение).
	DefaultLimitDownFactor = 0.5
	// DefaultLimitUpFactor — множитель восстановления ×1.25: подъём
	// медленнее снижения — резолвер только что показал перегрузку, быстрый
	// возврат даёт осцилляции «снизили → вернули → снова перегруз».
	DefaultLimitUpFactor = 1.25
	// DefaultLimitFloorDivisor — пол лимитов: base/делитель, но не меньше 1
	// (для 2000/10000 — 20/100): на «дне» конвейер продолжает работать,
	// полный останов (лимит 1/1) зарезал бы пропускную способность ради
	// резолвера, который и так лежит.
	DefaultLimitFloorDivisor = 100
)

// LimitOptions — параметры LimitResolver. Нулевые значения заменяются
// дефолтами (withDefaults, стиль Options клиента).
type LimitOptions struct {
	// Concurrency — потолок одновременных Exchange нижнего резолвера
	// (dns.concurrency). По умолчанию 2000.
	Concurrency int
	// MaxQPS — средний темп обменов в секунду (dns.max_qps).
	// По умолчанию 10000.
	MaxQPS int
	// Burst — ёмкость токен-ведра: сколько обменов разрешено подряд без
	// ожидания (короткие всплески конвейера). По умолчанию равен
	// Concurrency — «полная волна» всех слотов разом; больше бессмысленно:
	// одновременно летит не больше Concurrency обменов.
	Burst int
	// DisableAAAA — отвечать на AAAA-вопросы локальным пустым NOERROR
	// без похода в резолвер. Ставится из dns.ipv6=false (NewLimitOptions):
	// «Если на сервере нет IPv6 — AAAA-запросы отключаются конфигом»
	// (ТЗ 5.4); без IPv6 краулер не проверяет AAAA-адреса и по HTTP.
	DisableAAAA bool

	// AdaptiveWindow — окно доли таймаутов, обменов. По умолчанию 200.
	AdaptiveWindow int
	// AdaptiveHighRatio — порог снижения (доля ∈ (0, 1]). По умолчанию 0.10.
	AdaptiveHighRatio float64
	// AdaptiveLowRatio — порог восстановления (доля ∈ (0, high)).
	// По умолчанию 0.02.
	AdaptiveLowRatio float64
	// AdaptiveDownFactor — множитель снижения (∈ (0, 1)). По умолчанию 0.5.
	AdaptiveDownFactor float64
	// AdaptiveUpFactor — множитель восстановления (∈ (1, 10]).
	// По умолчанию 1.25.
	AdaptiveUpFactor float64
	// AdaptiveFloorDivisor — пол лимитов: base/делитель, не меньше 1.
	// По умолчанию 100.
	AdaptiveFloorDivisor int
}

// NewLimitOptions собирает параметры из секции dns: concurrency и max_qps —
// плановые лимиты (ТЗ 5.10, есть с T-006), ipv6=false — отключение
// AAAA-вопросов. Механика адаптива — дефолты LimitOptions (константы).
func NewLimitOptions(cfg config.DNS) LimitOptions {
	return LimitOptions{
		Concurrency: cfg.Concurrency,
		MaxQPS:      cfg.MaxQPS,
		DisableAAAA: !cfg.IPv6,
	}
}

// withDefaults заменяет нулевые и отрицательные значения дефолтами
// (стиль Options.withDefaults: «0 — значение по умолчанию»).
func (o LimitOptions) withDefaults() LimitOptions {
	if o.Concurrency <= 0 {
		o.Concurrency = DefaultLimitConcurrency
	}
	if o.MaxQPS <= 0 {
		o.MaxQPS = DefaultLimitQPS
	}
	if o.Burst <= 0 {
		o.Burst = o.Concurrency
	}
	if o.AdaptiveWindow <= 0 {
		o.AdaptiveWindow = DefaultLimitWindow
	}
	if o.AdaptiveHighRatio <= 0 {
		o.AdaptiveHighRatio = DefaultLimitHighRatio
	}
	if o.AdaptiveLowRatio <= 0 {
		o.AdaptiveLowRatio = DefaultLimitLowRatio
	}
	if o.AdaptiveDownFactor <= 0 {
		o.AdaptiveDownFactor = DefaultLimitDownFactor
	}
	if o.AdaptiveUpFactor <= 0 {
		o.AdaptiveUpFactor = DefaultLimitUpFactor
	}
	if o.AdaptiveFloorDivisor <= 0 {
		o.AdaptiveFloorDivisor = DefaultLimitFloorDivisor
	}
	return o
}

// validate проверяет значения после подстановки дефолтов. Ловит то, что
// нормализация не исправляет осмысленно: перевёрнутый гистерезис,
// немультипликативные факторы.
func (o LimitOptions) validate() error {
	if o.Concurrency < 1 {
		return fmt.Errorf("dnsclient: лимит concurrency %d: ожидается ≥ 1", o.Concurrency)
	}
	if o.MaxQPS < 1 {
		return fmt.Errorf("dnsclient: лимит max_qps %d: ожидается ≥ 1", o.MaxQPS)
	}
	if o.Burst < 1 {
		return fmt.Errorf("dnsclient: лимит burst %d: ожидается ≥ 1", o.Burst)
	}
	if o.AdaptiveWindow < 2 {
		return fmt.Errorf("dnsclient: окно адаптива %d: ожидается ≥ 2", o.AdaptiveWindow)
	}
	if o.AdaptiveHighRatio <= 0 || o.AdaptiveHighRatio > 1 {
		return fmt.Errorf("dnsclient: порог снижения %v: ожидается (0, 1]", o.AdaptiveHighRatio)
	}
	if o.AdaptiveLowRatio <= 0 || o.AdaptiveLowRatio >= o.AdaptiveHighRatio {
		return fmt.Errorf("dnsclient: порог восстановления %v: ожидается (0, %v)", o.AdaptiveLowRatio, o.AdaptiveHighRatio)
	}
	if o.AdaptiveDownFactor <= 0 || o.AdaptiveDownFactor >= 1 {
		return fmt.Errorf("dnsclient: фактор снижения %v: ожидается (0, 1)", o.AdaptiveDownFactor)
	}
	if o.AdaptiveUpFactor <= 1 || o.AdaptiveUpFactor > 10 {
		return fmt.Errorf("dnsclient: фактор восстановления %v: ожидается (1, 10]", o.AdaptiveUpFactor)
	}
	if o.AdaptiveFloorDivisor < 1 {
		return fmt.Errorf("dnsclient: делитель пола лимитов %d: ожидается ≥ 1", o.AdaptiveFloorDivisor)
	}
	return nil
}

// limitClock — время для лимитёра: продакшн — реальные часы и таймер,
// юнит-тесты подставляют фальшивые (детерминированный темп без реальных
// пауз). Тот же приём, что exchangeFunc у клиента: шов без зависимости
// от интерфейса снаружи пакета.
type limitClock struct {
	now   func() time.Time
	sleep func(ctx context.Context, d time.Duration) error
}

// limitRealClock — реальные часы: sleep прерывается отменой ctx.
func limitRealClock() limitClock {
	return limitClock{
		now: func() time.Time { return time.Now() },
		sleep: func(ctx context.Context, d time.Duration) error {
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
		},
	}
}

// limitSemaphore — счётный семафор параллелизма с изменяемым потолком
// (потолок опускает адаптив). Пробуждение ждущих — close канала поколения
// («разбудить всех»): см. комментарий пакета — массовых очередей тут не
// планируется, а широковещательное пробуждение не теряет событий.
type limitSemaphore struct {
	mu      sync.Mutex
	cur     int // выдано слотов
	max     int // текущий потолок
	waiters int // ждущих acquire (чтобы не будить зря)
	change  chan struct{}
}

// newLimitSemaphore создаёт семафор с потолком max.
func newLimitSemaphore(max int) *limitSemaphore {
	return &limitSemaphore{max: max, change: make(chan struct{})}
}

// acquire занимает слот; при исчерпании потолка ждёт освобождения,
// изменения потолка или отмены ctx (отменившийся не занимает слот —
// «не терять слот навсегда»).
func (s *limitSemaphore) acquire(ctx context.Context) error {
	for {
		s.mu.Lock()
		if s.cur < s.max {
			s.cur++
			s.mu.Unlock()
			return nil
		}
		s.waiters++
		ch := s.change
		s.mu.Unlock()

		select {
		case <-ch:
		case <-ctx.Done():
			s.mu.Lock()
			s.waiters--
			s.mu.Unlock()
			return ctx.Err()
		}

		s.mu.Lock()
		s.waiters--
		s.mu.Unlock()
	}
}

// release возвращает слот и будит ждущих.
func (s *limitSemaphore) release() {
	s.mu.Lock()
	// Страховка от неспаренного release: отрицательный cur сломал бы
	// инвариант cur ≤ max (потолок перестал бы ограничивать).
	if s.cur > 0 {
		s.cur--
	}
	s.wake()
	s.mu.Unlock()
}

// setMax меняет потолок: повышение и понижение будят ждущих (при понижении
// летящие обмены завершаются как есть — слоты не отбираются).
func (s *limitSemaphore) setMax(max int) {
	s.mu.Lock()
	if max != s.max {
		s.max = max
		s.wake()
	}
	s.mu.Unlock()
}

// state возвращает текущие cur и max (снимок для Stats).
func (s *limitSemaphore) state() (cur, max int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cur, s.max
}

// wake закрывает канал поколения; вызывающий обязан держать s.mu.
func (s *limitSemaphore) wake() {
	if s.waiters > 0 {
		ch := s.change
		s.change = make(chan struct{})
		close(ch)
	}
}

// limitBucket — токен-ведро темпа QPS с ленивым пополнением: токены
// начисляются в момент обращения за (now−last)×rate, без фоновых горутин.
//
// Выбор против альтернатив: фиксированное окно «N в секунду» режет
// естественные всплески конвейера (пачка субдоменов домена) и добавляет
// джиттер на границах окон; скользящее окно честнее, но требует памяти на
// метки времени и даёт ту же жёсткость. Токен-ведро держит средний темп
// max_qps и допускает короткий всплеск до burst — ровно режим «пачка
// уходит сразу, дальше темп выравнивается».
//
// Ожидание токена — резервирование, а не поллинг: нехватка списывается с
// баланса сразу (баланс уходит в минус — это долг перед ведром), момент
// своей выдачи ждущий вычисляет один раз под мьютексом и спит до него
// без пробуждений. Поллинг «проснуться к токену всем и пересчитать паузу»
// стоил бы ~N пробуждений на каждый выданный токен (N — очередь ждущих,
// до concurrency=2000): при 10000 QPS — миллионы пустых таймеров в
// секунду плюс джиттер выдачи (свежепришедший перехватывает токен у
// разбуженного).
type limitBucket struct {
	mu     sync.Mutex
	clock  limitClock
	rate   float64 // токенов в секунду (эффективный QPS)
	burst  float64 // ёмкость ведра
	tokens float64
	last   time.Time // момент последнего начисления
}

// newLimitBucket создаёт полное ведро (burst токенов): плановая волна
// старта конвейера уходит без ожидания.
func newLimitBucket(qps float64, burst int, clock limitClock) *limitBucket {
	return &limitBucket{
		clock:  clock,
		rate:   qps,
		burst:  float64(burst),
		tokens: float64(burst),
		last:   clock.now(),
	}
}

// take забирает токен; при пустом ведре ждёт начисления «своего» токена
// или отмены ctx (ожидающий токен не занимает его навсегда — семафор
// возвращается вызывающим по ошибке). Момент выдачи резервируется до
// ожидания (см. комментарий limitBucket): каждый ждущий спит ровно до
// своей метки и просыпается один раз. Отмена возвращает резерв —
// отменённое ожидание не тратит пропускную способность и не удлиняет
// ожидание очереди за ним.
func (b *limitBucket) take(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	wait, ok := b.reserve()
	if ok {
		return nil
	}
	if err := b.clock.sleep(ctx, wait); err != nil {
		b.unreserve()
		return err
	}
	// К пробуждению собственный долг погашен начислением (момент выдачи
	// вычислен под мьютексом по балансу с учётом всех предыдущих
	// резервов): ждущий проходит без повторной проверки общего баланса —
	// в нём остались долги очереди за ним, и такая проверка отправила бы
	// его в конец общего ожидания, ссыпая пачку в один всплеск. Темп,
	// сниженный адаптивом за время ожидания, даёт одноразовый допуск
	// одному ждущему — дешевле, чем повторные пробуждения.
	return nil
}

// reserve списывает токен без ожидания: при нехватке баланс уходит в
// минус (резерв будущего начисления); ok=false — токена нет, wait —
// время до начисления «своего» токена по текущему темпу (учтены резервы
// всех вставших в очередь раньше — выдача в порядке прихода, без
// перехвата токена свежепришедшим).
func (b *limitBucket) reserve() (time.Duration, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill()
	b.tokens--
	if b.tokens >= 0 {
		return 0, true
	}
	// (−tokens)/rate секунд до начисления «своего» токена; минимум 1 нс —
	// защита от нулевого ожидания на округлении (иначе take крутился бы
	// вхолостую).
	wait := time.Duration(-b.tokens / b.rate * float64(time.Second))
	if wait < 1 {
		wait = 1
	}
	return wait, false
}

// unreserve возвращает резерв отменённого ожидания: баланс растёт на
// один токен (не выше ёмкости ведра) — отмена не оставляет долг, который
// иначе удлинял бы ожидание очереди за отменённым.
func (b *limitBucket) unreserve() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill()
	b.tokens = math.Min(b.burst, b.tokens+1)
}

// setRate меняет темп (адаптив): сначала начисляет токены по старому темпу.
func (b *limitBucket) setRate(qps float64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill()
	b.rate = qps
}

// refill начисляет токены по прошедшему времени; вызывающий держит b.mu.
func (b *limitBucket) refill() {
	if now := b.clock.now(); now.After(b.last) {
		b.tokens = math.Min(b.burst, b.tokens+now.Sub(b.last).Seconds()*b.rate)
		b.last = now
	}
}

// limitAdaptive — адаптивное снижение нагрузки по доле таймаутов (ТЗ 5.4)
// в скользящем окне последних AdaptiveWindow обменов.
//
// Механика: кольцевой буфер исходов (таймаут/нет) со счётчиками — доля
// считается за O(1). Решение принимается после каждого обмена, но только
// когда окно заполнено (иначе первый же таймаут на старте урезал бы лимиты
// по статистике из одного события).
//
// Направления (обоснование): снижение мультипликативное и решительное
// (×0.5) — пока резолвер таймаутит, каждый обмен жжёт бюджет попыток и
// держит слот, вялое снижение растягивает аварию; восстановление осторожнее
// (×1.25) — резолвер только что был перегружен, быстрый возврат даёт
// осцилляцию. Между порогами — зона нечувствительности (гистерезис).
//
// Пол: base/DefaultLimitFloorDivisor (не меньше 1) — на «дне» конвейер
// продолжает работать, пока резолвер восстанавливается.
//
// Таймауты считаются только IsTimeout: отказы протокола/соединения — другая
// семантика (резолвер не «не успевает», а недоступен/бит), ТЗ 5.4 привязывает
// снижение именно к доле таймаутов.
type limitAdaptive struct {
	mu              sync.Mutex
	window          []bool // кольцо исходов: true — таймаут
	next            int    // позиция старейшей записи
	filled          bool   // окно заполнено целиком
	timeouts, total int

	baseConc int
	minConc  int
	conc     int
	baseQPS  float64
	minQPS   float64
	qps      float64

	high, low float64
	down, up  float64
}

// newLimitAdaptive готовит адаптив с базовыми лимитами (пол — base/div).
func newLimitAdaptive(o LimitOptions) *limitAdaptive {
	minConc := o.Concurrency / o.AdaptiveFloorDivisor
	if minConc < 1 {
		minConc = 1
	}
	minQPS := float64(o.MaxQPS) / float64(o.AdaptiveFloorDivisor)
	if minQPS < 1 {
		minQPS = 1
	}
	return &limitAdaptive{
		window:   make([]bool, o.AdaptiveWindow),
		baseConc: o.Concurrency,
		minConc:  minConc,
		conc:     o.Concurrency,
		baseQPS:  float64(o.MaxQPS),
		minQPS:   minQPS,
		qps:      float64(o.MaxQPS),
		high:     o.AdaptiveHighRatio,
		low:      o.AdaptiveLowRatio,
		down:     o.AdaptiveDownFactor,
		up:       o.AdaptiveUpFactor,
	}
}

// record добавляет исход обмена и возвращает эффективные лимиты; changed —
// лимиты изменились (вызывающему достаточно применить их только тогда).
func (a *limitAdaptive) record(timedOut bool) (conc int, qps float64, changed bool) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.filled {
		// Окно заполнено: вытесняем старейшую запись.
		if a.window[a.next] {
			a.timeouts--
		}
	} else {
		a.total++
	}
	a.window[a.next] = timedOut
	if timedOut {
		a.timeouts++
	}
	a.next++
	if a.next == len(a.window) {
		a.next = 0
	}
	if !a.filled && a.total == len(a.window) {
		a.filled = true
	}
	if !a.filled {
		return a.conc, a.qps, false
	}

	ratio := float64(a.timeouts) / float64(a.total)
	switch {
	case ratio >= a.high:
		// Снижение: вниз с округлением к меньшему, пол держит.
		newConc := max(a.minConc, int(float64(a.conc)*a.down))
		newQPS := math.Max(a.minQPS, a.qps*a.down)
		if newConc != a.conc || newQPS != a.qps {
			a.conc, a.qps = newConc, newQPS
			changed = true
		}
	case ratio <= a.low && (a.conc < a.baseConc || a.qps < a.baseQPS):
		// Восстановление: вверх с округлением к большему (ceil даёт рост
		// даже с 1), потолок — настроенные лимиты.
		newConc := min(a.baseConc, int(math.Ceil(float64(a.conc)*a.up)))
		newQPS := math.Min(a.baseQPS, a.qps*a.up)
		if newConc != a.conc || newQPS != a.qps {
			a.conc, a.qps = newConc, newQPS
			changed = true
		}
	}
	return a.conc, a.qps, changed
}

// snapshot возвращает текущее состояние (Stats): эффективные и базовые
// лимиты, состояние окна.
func (a *limitAdaptive) snapshot() (conc, baseConc int, qps, baseQPS float64, windowSize, windowTimeouts int, filled bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.conc, a.baseConc, a.qps, a.baseQPS, a.total, a.timeouts, a.filled
}

// LimitStats — снимок состояния лимитёра для мониторинга (экспорт метрик —
// T-077; здесь только данные, без зависимости от внутреннего/metrics).
type LimitStats struct {
	Concurrency    int     // эффективный потолок параллелизма (после адаптива)
	ConcurrencyCfg int     // настроенный потолок (dns.concurrency)
	QPS            float64 // эффективный темп (после адаптива)
	QPSCfg         float64 // настроенный темп (dns.max_qps)
	InFlight       int     // обменов в нижнем резолвере сейчас
	WindowFilled   bool    // окно адаптива заполнено
	WindowSize     int     // обменов в окне (пока наполняется — меньше)
	WindowTimeouts int     // таймаутов в окне
	Exchanges      int64   // завершено обменов через лимитёр
	Timeouts       int64   // из них — таймауты
	Errors         int64   // из них — прочие ошибки
	AAAAIgnored    int64   // AAAA-вопросов отвечено локально (без резолвера)
}

// LimitResolver — декоратор Resolver с лимитами параллелизма/QPS,
// адаптивным снижением по доле таймаутов и отключением AAAA (ТЗ 5.4,
// dns.concurrency/max_qps/ipv6 из ТЗ 5.10). Безопасен для одновременного
// использования из горутин.
type LimitResolver struct {
	next     Resolver
	opt      LimitOptions
	sem      *limitSemaphore
	bucket   *limitBucket
	adaptive *limitAdaptive

	exchanges   atomic.Int64
	timeouts    atomic.Int64
	errors      atomic.Int64
	aaaaIgnored atomic.Int64
}

// compile-time check: *LimitResolver — Resolver.
var _ Resolver = (*LimitResolver)(nil)

// NewLimitResolver оборачивает next лимитами по opts (см. NewLimitOptions
// для сборки из конфигурации). Ошибка — nil next или бессмысленные
// параметры (после подстановки дефолтов).
func NewLimitResolver(next Resolver, opts LimitOptions) (*LimitResolver, error) {
	return newLimitResolver(next, opts, limitRealClock())
}

// newLimitResolver — общая часть с инъекцией часов (тесты).
func newLimitResolver(next Resolver, opts LimitOptions, clock limitClock) (*LimitResolver, error) {
	if next == nil {
		return nil, errors.New("dnsclient: NewLimitResolver: нижний резолвер не задан")
	}
	opts = opts.withDefaults()
	if err := opts.validate(); err != nil {
		return nil, err
	}
	return &LimitResolver{
		next:     next,
		opt:      opts,
		sem:      newLimitSemaphore(opts.Concurrency),
		bucket:   newLimitBucket(float64(opts.MaxQPS), opts.Burst, clock),
		adaptive: newLimitAdaptive(opts),
	}, nil
}

// Exchange выполняет вопрос через лимиты: слот параллелизма → токен темпа →
// нижний резолвер. Отмена ctx прерывает ожидание в любой очереди; занятые
// к этому моменту слоты возвращаются (defer), токен в очередь не ставится.
//
// AAAA при DisableAAAA — локальный пустой NOERROR (NODATA: «имя есть,
// записей этого типа нет»): без похода в сеть, без лимитов и окна адаптива.
// Attempts 0, TCP false, Elapsed 0 — обмена не было.
func (r *LimitResolver) Exchange(ctx context.Context, q Question) (*Result, error) {
	if r.opt.DisableAAAA && q.Type == dns.TypeAAAA {
		r.aaaaIgnored.Add(1)
		return &Result{RCode: RCodeNOERROR}, nil
	}
	if err := r.sem.acquire(ctx); err != nil {
		return nil, err
	}
	defer r.sem.release()
	if err := r.bucket.take(ctx); err != nil {
		return nil, err
	}
	res, err := r.next.Exchange(ctx, q)
	r.observe(err)
	return res, err
}

// observe учитывает исход обмена: счётчики и адаптив; при изменении
// эффективных лимитов применяет их к семафору и ведру.
func (r *LimitResolver) observe(err error) {
	r.exchanges.Add(1)
	timedOut := IsTimeout(err)
	if err != nil {
		if timedOut {
			r.timeouts.Add(1)
		} else {
			r.errors.Add(1)
		}
	}
	conc, qps, changed := r.adaptive.record(timedOut)
	if changed {
		r.sem.setMax(conc)
		r.bucket.setRate(qps)
	}
}

// Stats возвращает снимок состояния (точки для мониторинга T-077:
// эффективные лимиты, окно адаптива, счётчики).
func (r *LimitResolver) Stats() LimitStats {
	conc, baseConc, qps, baseQPS, windowSize, windowTimeouts, filled := r.adaptive.snapshot()
	cur, _ := r.sem.state()
	return LimitStats{
		Concurrency:    conc,
		ConcurrencyCfg: baseConc,
		QPS:            qps,
		QPSCfg:         baseQPS,
		InFlight:       cur,
		WindowFilled:   filled,
		WindowSize:     windowSize,
		WindowTimeouts: windowTimeouts,
		Exchanges:      r.exchanges.Load(),
		Timeouts:       r.timeouts.Load(),
		Errors:         r.errors.Load(),
		AAAAIgnored:    r.aaaaIgnored.Load(),
	}
}

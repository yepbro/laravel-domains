// Надёжность доставки на Redis Streams (T-025, ТЗ 4.2 и 3.4): политика
// усечения стримов MAXLEN по умолчанию, явный паттерн «XACK только после
// записи результата в БД» (Processing) и возврат зависших записей через
// XAUTOCLAIM со счётчиком попыток и переводом в q:dead.

package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// DefaultMaxAttempts — максимальное число попыток доставки задачи по
// умолчанию (ТЗ 4.2: «максимальное число попыток на задачу — настраивается
// (по умолчанию 3), далее q:dead»). Попытка — доставка записи потребителю:
// первое чтение группой плюс каждый возврат зависшей записи клеймом.
const DefaultMaxAttempts = 3

// DefaultClaimMinIdle — минимальный простой pending-записи до возврата в
// работу (MINIDLE XAUTOCLAIM) по умолчанию. Должен превышать худшее
// время, в течение которого живой воркер держит доставку неподтверждённой,
// иначе чужая работа перехватывается на ходу: это не время одной задачи,
// а окно потребления — Consume-окно записей, обрабатываемых циклом
// последовательно при минимальном параллелизме. Худший настроенный
// случай — рендер: окно 16 × таймаут 25 с при contexts = 1 ≈ 6,7 мин
// (q:http при concurrency 1 — 16 × 20 с ≈ 5,3 мин); 10 минут даёт
// полуторакратный запас. Настройка — redis.claim_min_idle (Q-03).
const DefaultClaimMinIdle = 10 * time.Minute

// Пределы длины стримов политики MAXLEN по умолчанию (ТЗ 4.2: «MAXLEN на
// каждом стриме»). MAXLEN ~ — аварийный потолок длины: без него стримы
// растут неограниченно (350 млн доменов, несколько стримов на домен),
// а неусечённая необработанная запись не теряется бесследно: источник
// истины — PostgreSQL, задачу восстанавливает планировщик по lease
// (ТЗ 3.4, 5.3). Рабочая граница глубины — гейт backpressure: его порог
// ограничен половиной предела стрима (BackpressureGate.watermarkFor),
// потолок достижим лишь при отключённом гейте; подтверждённый префикс
// между гейтом и потолком освобождает TrimLoop (T-141, trim.go).
//
// Пределы согласованы с бюджетом Redis ≤ 0,5 ГБ (ТЗ 3.2) по фактическим
// размерам записей. Замер T-141 (Redis 7.4, MEMORY USAGE): пачка q:dns
// из 1000 доменов (scheduler.batch_size) ≈ 64 КиБ — ~66 Б на домен
// с накладными стрима; прежняя оценка «237 КиБ на пачку» относилась
// к пачкам эпохи batch_size 5000 (инцидентные 1735 батчей = 410 МиБ ≈
// 3,7 тыс. доменов в пачке в среднем). Штучные записи 0,3–0,7 КиБ
// (замер T-092, docs/load-measurements.md §11). Сумма потолков обязана
// укладываться в бюджет: при noeviction потолок выше бюджета означал бы
// отказ записи (OOM) раньше обрезки — ровно это случилось в инциденте
// T-093. Арифметика:
//
//	q:dns     250 пачек × 64 КиБ ≈ 16 МиБ (гейт 125 пачек ≈ 8 МиБ)
//	q:http    150 000 × 0,7 КиБ ≈ 103 МиБ (гейт 75 000 ≈ 52 МиБ)
//	q:render  150 000 × 0,7 КиБ ≈ 103 МиБ (гейт 75 000 ≈ 52 МиБ)
//	q:analyze 150 000 × 0,7 КиБ ≈ 103 МиБ (гейт 75 000 ≈ 52 МиБ)
//	q:dead    10 000 × ~0,2 КиБ ≈ 2 МиБ (без payload, T-105)
//
//	Итого потолки ≈ 327 МиБ < 512 МиБ (запас ≈ 185 МиБ — служебная
//	память Redis, фрагментация, буферы клиентов и рост записей с
//	длинными именами; межобрезочный остаток TrimLoop сам ограничен
//	потолками); одновременно гейты-full ≈ 164 МиБ. Размер пачки
//	настраивается (scheduler.batch_size 1000..10000): потолок q:dns
//	пропорционален ей — инвариант «сумма потолков ≤ бюджета» для всей
//	нормативной вилки пачки (худший случай 10 000 доменов ≈ 467 МиБ)
//	пин-тестами TestStreamPolicyFitsBudget (reliab_test.go) и
//	TestSchedulerBatchFitsStreamBudget (config_test.go, от границы
//	валидации).
const (
	// defaultMaxLenDNS — записи q:dns — пачки scheduler.batch_size
	// (1000) доменов ≈ 64 КиБ (замер T-141): 250 пачек ≈ 16 МиБ потолок,
	// гейт — 125 пачек ≈ 8 МиБ ≈ 125 тыс. доменов «в полёте» при
	// остановке DNS-этапа (≈10 минут работы этапа по замеру T-092).
	// 250, а не больше: потолок пропорционален настраиваемой пачке —
	// при максимальной нормативной (10 000 доменов, ТЗ 5.3) 250 пачек
	// ≈ 157 МиБ, и сумма потолков ≈ 467 МиБ остаётся в бюджете.
	defaultMaxLenDNS = 250
	// defaultMaxLenUnit — штучные задачи q:http, q:render, q:analyze
	// (замер 0,3–0,7 КиБ, не «~100–200 байт» прежней оценки): потолок
	// 150 000 × 0,7 КиБ ≈ 103 МиБ на стрим; три штучных стрима и q:dns
	// в сумме ≈ 327 МиБ бюджета. Гейт — min(watermark, половина) =
	// 75 000 записей ≈ 52 МиБ.
	defaultMaxLenUnit = 150_000
	// defaultMaxLenDead — поток q:dead редок (исчерпанные попытки), а
	// ручному разбору нужна длинная история. Записи не несут payload
	// исходных задач (T-105: идентификаторы предмета вместо сообщения —
	// пачки q:dns до ~250 КБ давали worst-case 10 000 × 250 КБ ≈ 2,5 ГБ
	// против бюджета Redis ≤ 0,5 ГБ, ТЗ 3.2): запись — ~150–300 Б,
	// 10 000 ≈ 2–3 МБ, на два порядка ниже бюджета.
	defaultMaxLenDead = 10_000
)

// Бюджетные оценки расхода памяти Redis на одну запись стрима (замер
// T-141, Redis 7.4, MEMORY USAGE; штучные — верх диапазра замера T-092).
// Экспортированы для пин-теста инварианта бюджета (TestStreamPolicyFitsBudget:
// потолок q:dns зависит от настраиваемого scheduler.batch_size) и для
// арифметики docs/redis.md §3.1.
const (
	// DNSStreamBytesPerDomain — байт на домен пачки q:dns с накладными
	// стрима: запись 1000 доменов ≈ 64 КиБ (JSON ~60 Б на домен + узлы
	// списка записей).
	DNSStreamBytesPerDomain int64 = 66
	// UnitStreamBytesWorst — худшая штучная запись q:http/q:render/
	// q:analyze (0,7 КиБ; типичная — 0,3–0,5 КиБ).
	UnitStreamBytesWorst int64 = 717
	// DeadStreamBytesWorst — запись q:dead без payload (T-105,
	// ~150–300 Б).
	DeadStreamBytesWorst int64 = 205
)

// DefaultStreamMaxLen — предел длины стрима из политики T-025: применяется
// RedisQueue.Publish для каждого XADD, когда явный предел не задан
// (RedisOptions.MaxLen == 0). Экспортировано для конфигурации и будущих
// задач (интеграция, мониторинг), состав — константы выше.
func DefaultStreamMaxLen(stream string) int64 {
	switch stream {
	case StreamDNS:
		return defaultMaxLenDNS
	case StreamHTTP, StreamRender, StreamAnalyze:
		return defaultMaxLenUnit
	default: // StreamDead и всё прочее
		return defaultMaxLenDead
	}
}

// Processing — паттерн надёжной обработки одной доставки (ТЗ 4.2: «XACK
// только после записи результата в БД»; ТЗ 3.4): обработчик process
// выполняет работу и записывает результат в БД, и лишь после его успешного
// завершения запись подтверждается. Обёртка гарантирует порядок физически:
// Ack недостижим раньше безошибочного возврата process.
//
// Ошибка process — возврат без XACK: запись остаётся в pending группы и
// вернётся повторной доставкой (Reliab.Claim), то есть доставка
// at-least-once и запись результата обязана быть идемпотентной (ТЗ 3.4).
// Ошибка Ack тоже возвращается — результат уже в БД, а запись осталась в
// pending и будет доставлена повторно: повторный вызов process обязан
// пройти без изменений (та же идемпотентность).
//
// Цикл воркера: Consume → Processing для каждой доставки → при ошибке —
// лог и продолжение (не останавливает остальные, ТЗ 3.4).
func Processing(ctx context.Context, q Queue, group string, d Delivery, process func(context.Context, Delivery) error) error {
	if process == nil {
		return fmt.Errorf("queue: Processing записи %s стрима %s: не задан обработчик", d.ID, d.Stream)
	}
	return ProcessingAttempt(ctx, q, group, d, 1, func(c context.Context, del Delivery, _ int) error {
		return process(c, del)
	})
}

// ProcessingAttempt — паттерн Processing для обработчика, которому нужен
// номер попытки доставки (T-148): попытка — счётчик доставок записи в PEL
// группы (первое чтение XREADGROUP = 1, каждый клейм XAUTOCLAIM +1,
// Claimed.Attempts). Семантика подтверждения та же, что у Processing:
// Ack только после безошибочного возврата process, ошибка — без XACK
// (повторная доставка); классификация ошибок по счётчику — дело
// обработчика (политика таймаутов рендера, T-148), обёртка только
// доставляет число. Обработчик nil — ошибка (стиль Processing).
func ProcessingAttempt(ctx context.Context, q Queue, group string, d Delivery, attempts int, process func(context.Context, Delivery, int) error) error {
	if process == nil {
		return fmt.Errorf("queue: Processing записи %s стрима %s: не задан обработчик", d.ID, d.Stream)
	}
	if err := process(ctx, d, attempts); err != nil {
		return fmt.Errorf("queue: запись %s стрима %s не подтверждена (без XACK, вернётся повторной доставкой): %w", d.ID, d.Stream, err)
	}
	return q.Ack(ctx, d.Stream, group, d.ID)
}

// claimStreams — окно go-redis для возврата зависших записей: XAUTOCLAIM
// (клейм, Redis 6.2+) и XPENDING EXTENDED (счётчик доставок). *redis.Client
// (queue.NewClient) и *redis.ClusterClient удовлетворяют интерфейсу;
// тесты подменяют фейком — фабрик New*Result у этих команд go-redis не
// имеет, значения задаются SetVal/SetErr заготовленных команд.
type claimStreams interface {
	XAutoClaim(ctx context.Context, a *redis.XAutoClaimArgs) *redis.XAutoClaimCmd
	XPendingExt(ctx context.Context, a *redis.XPendingExtArgs) *redis.XPendingExtCmd
}

// ReliabOptions — параметры возврата зависших записей (T-025).
type ReliabOptions struct {
	// MaxAttempts — максимум доставок задачи (первая + клеймы) до
	// перевода в q:dead; 0 — DefaultMaxAttempts.
	MaxAttempts int
	// MinIdle — минимальный простой pending-записи до клейма, когда Claim
	// вызван с minIdle 0; 0 — DefaultClaimMinIdle.
	MinIdle time.Duration
}

// Reliab — возврат зависших записей Redis Streams со счётчиком попыток
// (T-025, ТЗ 4.2: «зависшие сообщения забираются через XAUTOCLAIM …
// максимальное число попыток на задачу — настраивается, далее q:dead»).
// Работает с той же группой потребителей, из которой читают воркеры:
// записи, зависшие в pending (прочитаны и не подтверждены — воркер пал
// или завис), перезакрепляются на потребителя consumer и возвращаются в
// работу, пока не исчерпан лимит попыток; исчерпавшие лимит публикуются
// в q:dead (Dead) и подтверждаются в исходном стриме, покидая pending.
type Reliab struct {
	rdb claimStreams // XAUTOCLAIM + XPENDING
	q   Queue        // публикация Dead и XACK исходных записей
	opt ReliabOptions
}

// NewReliab строит слой надёжности поверх очереди q и клиента rdb — это
// должен быть один и тот же Redis (Dead публикуется и подтверждается через
// Queue, клейм и счётчик — напрямую): например NewReliab(rdb,
// NewRedisQueue(rdb, RedisOptions{}), opt). Нулевые значения опций
// заменяются значениями по умолчанию.
func NewReliab(rdb claimStreams, q Queue, opt ReliabOptions) *Reliab {
	if opt.MaxAttempts <= 0 {
		opt.MaxAttempts = DefaultMaxAttempts
	}
	if opt.MinIdle <= 0 {
		opt.MinIdle = DefaultClaimMinIdle
	}
	return &Reliab{rdb: rdb, q: q, opt: opt}
}

// Claimed — зависшая запись, возвращённая в работу (Reliab.Claim).
// Attempts — номер текущей попытки: счётчик доставок записи в PEL группы
// (первое чтение XREADGROUP + каждый клейм). Обрабатывается паттерном
// Processing, как свежая доставка: обработка и запись результата в БД →
// XACK; результат не подтверждён — счётчик вырастет на следующем клейме.
type Claimed struct {
	Delivery
	Attempts int
}

// Claim возвращает до n зависших записей стрима stream — прочитанных
// группой group и не подтверждённых не менее minIdle (0 — ReliabOptions.
// MinIdle), закрепляя их за потребителем consumer. Сканирование PEL
// начинается с курсора start ("" или "0-0" — с начала); next в возврате —
// курсор следующего сканирования ("0-0" — обход PEL завершён): вызывающий
// цикл продолжает с него, не перечитывая пропущенные (ещё не
// «созревшие» по MinIdle) записи с начала на каждом вызове.
//
// Выбор XAUTOCLAIM (а не XPENDING+XCLAIM): ТЗ 4.2 называет XAUTOCLAIM
// прямо; go-redis v9 поддерживает его полностью. Одна атомарная команда
// фильтрует PEL по простою, перезакрепляет записи и возвращает курсор
// обхода; XPENDING+XCLAIM потребовал бы двух раундов с окном гонки между
// списком и клеймом, а XCLAIM не фильтрует по простою. Дополнительно
// XAUTOCLAIM (Redis 7+) вычищает из PEL записи, уже усечённые MAXLEN:
// такие приходят с пустыми значениями и пропускаются.
//
// Счётчик попыток ведёт сам Redis — deliveries-счётчик PEL записи
// (поле RetryCount ответа XPENDING EXTENDED: инкрементируется на каждую
// доставку — первое чтение группы и каждый клейм). Тело записи стрима
// immutable, отдельные ключи-счётчики не заводятся: счётчик живёт в PEL,
// переживает рестарты воркеров и согласован при многих восстановителях.
// Счётчик читается XPENDING EXTENDED по каждой вернувшейся записи
// (диапазон id..id: XAUTOCLAIM его не возвращает); чтение без побочных
// эффектов, поэтому безопасно при гонках с другими восстановителями.
//
// Запись со счётчиком выше MaxAttempts не возвращается, а переводится в
// q:dead: Claim публикует Dead (task_id и идентификаторы предмета из
// тела, без исходного payload — T-105) и подтверждает (XACK) запись
// в исходном стриме — сообщение покидает pending. Dead публикуется до
// XACK: сбой XACK оставит запись в pending и даст дубль в q:dead на
// следующем проходе — поток q:dead редок и разбирается вручную, дубль
// предпочтительнее потери (ТЗ 3.4).
//
// Ошибка XPENDING одной записи не рушит пачку: XAUTOCLAIM уже закрепил
// записи за потребителем и сбросил им idle — потеря всей пачки на сбое
// одной проверки означала бы ещё один MinIdle простоя для записей,
// счётчик которых известен. Ошибки собираются в возврат (errors.Join),
// валидная часть отдаётся; запись с неизвестным счётчиком остаётся
// закреплённой и вернётся следующим клеймом после нового MinIdle.
// Счётчик читается XPENDING EXTENDED по каждой вернувшейся записи
// (диапазон id..id: XAUTOCLAIM его не возвращает); чтение без побочных
// эффектов, поэтому безопасно при гонках с другими восстановителями.
//
// Пустой результат без ошибки — зависших записей нет.
func (r *Reliab) Claim(ctx context.Context, stream, group, consumer string, minIdle time.Duration, n int, start string) ([]Claimed, string, error) {
	if !knownStream(stream) {
		return nil, "", fmt.Errorf("queue: неизвестный стрим %q", stream)
	}
	if group == "" {
		return nil, "", fmt.Errorf("queue: не задана группа потребителей стрима %s", stream)
	}
	if consumer == "" {
		return nil, "", fmt.Errorf("queue: не задано имя потребителя группы %s", group)
	}
	if minIdle <= 0 {
		minIdle = r.opt.MinIdle
	}
	if n < 1 {
		return nil, "", fmt.Errorf("queue: размер пачки клейма %d < 1", n)
	}
	if start == "" {
		start = "0-0"
	}
	msgs, next, err := r.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   stream,
		Group:    group,
		Consumer: consumer,
		MinIdle:  minIdle,
		Start:    start, // курсор вызывающего цикла: продолжение обхода PEL, а не всегда с начала
		Count:    int64(n),
	}).Result()
	if err != nil {
		return nil, "", fmt.Errorf("queue: claim %s (группа %s): %w", stream, group, err)
	}
	if next == "" {
		next = "0-0" // обход PEL завершён — следующий с начала
	}
	out := make([]Claimed, 0, len(msgs))
	var errs []error
	for _, m := range msgs {
		d := Delivery{Stream: stream, ID: m.ID, Payload: payloadBytes(m.Values)}
		if len(d.Payload) == 0 {
			// Запись усечена MAXLEN, пока висела в pending: значения
			// пусты, XAUTOCLAIM уже вычистил её из PEL — не обрабатываем.
			continue
		}
		pending, err := r.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: stream,
			Group:  group,
			Start:  m.ID,
			End:    m.ID, // ровно эта запись: счётчик чтения ради, без разбора чужого pending
			Count:  1,
		}).Result()
		if err != nil {
			errs = append(errs, fmt.Errorf("queue: счётчик доставок записи %s (%s): %w", m.ID, stream, err))
			continue
		}
		if len(pending) == 0 {
			// Запись покинула PEL между командами (подтверждена тем, кто
			// её ещё обрабатывал) — уже разобрана, пропускаем.
			continue
		}
		attempts := int(pending[0].RetryCount)
		if attempts > r.opt.MaxAttempts {
			if err := r.bury(ctx, group, d, attempts); err != nil {
				errs = append(errs, err)
			}
			continue
		}
		out = append(out, Claimed{Delivery: d, Attempts: attempts})
	}
	if len(out) == 0 {
		// Эквивалентные мутанты (T-M04): `> 0` → `>= 0`/`> -1` неотличимы —
		// errors.Join() без ошибок возвращает nil, ветка при пустых errs
		// совпадает с падением в return ниже. `> 1` (numbers/incrementer)
		// отличим одиночной ошибкой и не гасится.
		// mutator-disable-next-line expression/comparison, numbers/decrementer
		if len(errs) > 0 {
			return nil, next, errors.Join(errs...)
		}
		return nil, next, nil
	}
	return out, next, errors.Join(errs...)
}

// Process — паттерн Processing поверх очереди слоя: клеймнутая Claim
// запись обрабатывается тем же правилом «результат в БД → XACK» (ТЗ 4.2),
// что и свежие доставки воркера. RecoverLoop использует для повторов
// общий с воркером обработчик — идемпотентность записей (ТЗ 3.4) делает
// повтор безопасным.
func (r *Reliab) Process(ctx context.Context, group string, d Delivery, process func(context.Context, Delivery) error) error {
	return Processing(ctx, r.q, group, d, process)
}

// ProcessAttempt — Process для обработчика с номером попытки доставки
// (T-148): RecoverLoopAttempt передаёт Claimed.Attempts, обработчик
// воркера решает по счётчику политику ошибок (таймаут-тарпиты рендера).
func (r *Reliab) ProcessAttempt(ctx context.Context, group string, d Delivery, attempts int, process func(context.Context, Delivery, int) error) error {
	return ProcessingAttempt(ctx, r.q, group, d, attempts, process)
}

// bury переводит запись, исчерпавшую лимит попыток, в q:dead и
// подтверждает её в исходном стриме группы group (T-025): Dead — с
// task_id, источником и идентификаторами предмета задачи, выжатыми из
// тела исходного сообщения (deadSubjectOf), без payload — байтовая
// защита T-105 (см. Dead); Reason фиксирует причину — зависание без
// подтверждения (воркеры ошибки обработки очереди не сообщают, ТЗ 4.2,
// поэтому «последняя ошибка» сводится к факту исчерпания). Публикация
// Dead идёт до XACK (см. Claim).
func (r *Reliab) bury(ctx context.Context, group string, d Delivery, attempts int) error {
	taskID := taskIDOf(d.Payload)
	if taskID == "" {
		return fmt.Errorf("queue: запись %s стрима %s без task_id — в q:dead не переводится (сообщение вне форматов пакета), разберите вручную", d.ID, d.Stream)
	}
	dead := Dead{
		TaskID:   taskID,
		Stream:   d.Stream,
		EntryID:  d.ID,
		Attempts: attempts,
		Reason:   fmt.Sprintf("исчерпан лимит попыток %d: доставок без подтверждения — %d", r.opt.MaxAttempts, attempts),
	}
	// Идентификаторы предмета (T-105). Выжимка из чужого тела может дать
	// недопустимые значения (variant вне 0/1, имя длиннее лимита) —
	// Publish отклонил бы такую запись, XACK не состоялся бы, и задача
	// с task_id навсегда застряла бы в pending. Тогда переносим базу без
	// идентификаторов: q:dead — карантин, обязан принять любую задачу
	// с task_id (база всегда валидна — проверены полями bury/Claim).
	subject := dead
	deadSubjectOf(&subject, d.Stream, d.Payload)
	if subject.validate() == nil {
		dead = subject
	}
	if _, err := r.q.Publish(ctx, StreamDead, dead); err != nil {
		return fmt.Errorf("queue: публикация в q:dead записи %s стрима %s: %w", d.ID, d.Stream, err)
	}
	if err := r.q.Ack(ctx, d.Stream, group, d.ID); err != nil {
		return fmt.Errorf("queue: подтверждение перенесённой в q:dead записи %s стрима %s: %w", d.ID, d.Stream, err)
	}
	return nil
}

// taskIDOf достаёт task_id из тела записи — envelope всех сообщений пакета
// (message.go). Пустое значение — сообщение вне форматов пакета или битое:
// связать Dead с исходной задачей нельзя.
func taskIDOf(payload []byte) string {
	var env struct {
		TaskID string `json:"task_id"`
	}
	// Эквивалентный мутант (T-M04): вырезание return из ветки ошибки
	// неотличимо — Unmarshal при ошибке не заполняет поля (синтаксис
	// всего ввода проверяется до декодирования, сбой типа поле не
	// пишет), env.TaskID и так "".
	// mutator-disable-next-line branch/if
	if err := json.Unmarshal(payload, &env); err != nil {
		return ""
	}
	return env.TaskID
}

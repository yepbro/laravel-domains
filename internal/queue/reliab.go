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
// каждом стриме»). MAXLEN ~ усекает прежде всего историю подтвержденных
// записей — без усечения стримы растут неограниченно (350 млн доменов,
// несколько стримов на домен), а неусечённая необработанной запись не
// теряется бесследно: источник истины — PostgreSQL, задачу восстанавливает
// планировщик по lease (ТЗ 3.4, 5.3). Пределы выше рабочей глубины
// очередей — гейт backpressure срабатывает раньше обрезки: его порог
// ограничен половиной предела стрима (BackpressureGate.watermarkFor),
// поэтому в здоровой системе усекается только уже обработанное.
const (
	// defaultMaxLenDNS — записи q:dns — пачки до scheduler.batch_size
	// (5000) доменов, до ~250 КБ каждая; реальная глубина — десятки пачек,
	// 10 000 пачек = 50 млн доменов «в полёте» — больше одного прохода
	// корпуса, а память ограничена единицами гигабайт.
	defaultMaxLenDNS = 10_000
	// defaultMaxLenUnit — штучные задачи q:http, q:render, q:analyze
	// (~100–200 байт): 1 млн записей ≈ 150–200 МБ на стрим, на порядок
	// выше порога backpressure.
	defaultMaxLenUnit = 1_000_000
	// defaultMaxLenDead — поток q:dead редок (исчерпанные попытки), а
	// ручному разбору нужна длинная история; payload — исходное сообщение,
	// включая крупные пачки q:dns.
	defaultMaxLenDead = 100_000
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
	if err := process(ctx, d); err != nil {
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
// MinIdle), закрепляя их за потребителем consumer.
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
// q:dead: Claim публикует Dead (исходный payload без изменений, task_id
// из тела) и подтверждает (XACK) запись в исходном стриме — сообщение
// покидает pending. Dead публикуется до XACK: сбой XACK оставит запись в
// pending и даст дубль в q:dead на следующем проходе — поток q:dead редок
// и разбирается вручную, дубль предпочтительнее потери (ТЗ 3.4).
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
func (r *Reliab) Claim(ctx context.Context, stream, group, consumer string, minIdle time.Duration, n int) ([]Claimed, error) {
	if !knownStream(stream) {
		return nil, fmt.Errorf("queue: неизвестный стрим %q", stream)
	}
	if group == "" {
		return nil, fmt.Errorf("queue: не задана группа потребителей стрима %s", stream)
	}
	if consumer == "" {
		return nil, fmt.Errorf("queue: не задано имя потребителя группы %s", group)
	}
	if minIdle <= 0 {
		minIdle = r.opt.MinIdle
	}
	if n < 1 {
		return nil, fmt.Errorf("queue: размер пачки клейма %d < 1", n)
	}
	msgs, _, err := r.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   stream,
		Group:    group,
		Consumer: consumer,
		MinIdle:  minIdle,
		Start:    "0-0", // с начала PEL: закреплённые записи получают новое время доставки и повторно не находятся
		Count:    int64(n),
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("queue: claim %s (группа %s): %w", stream, group, err)
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
		if len(errs) > 0 {
			return nil, errors.Join(errs...)
		}
		return nil, nil
	}
	return out, errors.Join(errs...)
}

// Process — паттерн Processing поверх очереди слоя: клеймнутая Claim
// запись обрабатывается тем же правилом «результат в БД → XACK» (ТЗ 4.2),
// что и свежие доставки воркера. RecoverLoop использует для повторов
// общий с воркером обработчик — идемпотентность записей (ТЗ 3.4) делает
// повтор безопасным.
func (r *Reliab) Process(ctx context.Context, group string, d Delivery, process func(context.Context, Delivery) error) error {
	return Processing(ctx, r.q, group, d, process)
}

// bury переводит запись, исчерпавшую лимит попыток, в q:dead и
// подтверждает её в исходном стриме группы group (T-025): Dead — с task_id
// и payload исходного сообщения без изменений; Reason фиксирует причу —
// зависание без подтверждения (воркеры ошибки обработки очереди не
// сообщают, ТЗ 4.2, поэтому «последняя ошибка» сводится к факту
// исчерпания). Публикация Dead идёт до XACK (см. Claim).
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
		Payload:  json.RawMessage(d.Payload),
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
	if err := json.Unmarshal(payload, &env); err != nil {
		return ""
	}
	return env.TaskID
}

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
	"strings"
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
//	q:render  150 000 × 0,7 КиБ ≈ 103 МиБ (ворота ёмкости — отдельный
//	          render.queue_high_watermark, T-174: 10 000 ≈ 7 МиБ; 0 —
//	          общий → 75 000 ≈ 52 МиБ)
//	q:render-fallback 15 000 × 0,7 КиБ ≈ 10 МиБ (не гейтится: потолок —
//	          бюджет доли и темп воркера B, ТЗ 4.3 п.3)
//	q:analyze 150 000 × 0,7 КиБ ≈ 103 МиБ (гейт 75 000 ≈ 52 МиБ)
//	q:dead    10 000 × ~0,2 КиБ ≈ 2 МиБ (без payload, T-105)
//
//	Итого потолки ≈ 337 МиБ < 512 МиБ (запас ≈ 175 МиБ — служебная
//	память Redis, фрагментация, буферы клиентов и рост записей с
//	длинными именами; межобрезочный остаток TrimLoop сам ограничен
//	потолками); одновременно гейты-full ≈ 119 МиБ (T-174 понизил ворота
//	q:render с 52 до ≈7 МиБ). Размер пачки
//	настраивается (scheduler.batch_size 1000..10000): потолок q:dns
//	пропорционален ей — инвариант «сумма потолков ≤ бюджета» для всей
//	нормативной вилки пачки (худший случай 10 000 доменов ≈ 478 МиБ)
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
	// ≈ 157 МиБ, и сумма потолков ≈ 478 МиБ остаётся в бюджете.
	defaultMaxLenDNS = 250
	// defaultMaxLenUnit — штучные задачи q:http, q:render, q:analyze
	// (замер 0,3–0,7 КиБ, не «~100–200 байт» прежней оценки): потолок
	// 150 000 × 0,7 КиБ ≈ 103 МиБ на стрим; три штучных стрима, фолбэк и
	// q:dns в сумме ≈ 337 МиБ бюджета. Гейт q:http/q:analyze —
	// min(watermark, половина) = 75 000 записей ≈ 52 МиБ; ворота
	// ёмкости q:render — отдельный render.queue_high_watermark
	// (T-174: 10 000 по умолчанию, арифметика — в таблице шапки).
	defaultMaxLenUnit = 150_000
	// defaultMaxLenRenderFallback — очередь фолбэка рендера (T-237,
	// ТЗ 4.3): задачи, не прошедшие основной движок, дорабатывает
	// chromium-воркер B в ограниченном темпе (ФТ-13). Потолок — не
	// полный поток этапа, а его ограниченная доля, поэтому 15 000 —
	// десятая часть штучного потолка ≈ 10 МиБ. Бюджетное ограничение:
	// при худшей нормативной пачке q:dns (10 000 доменов, ТЗ 5.3) сумма
	// потолков ≈ 478 МиБ обязана остаться ниже 480 МиБ предела
	// пин-тестов (бюджет 512 МиБ, ТЗ 3.2) — 150 000, как у штучных
	// стримов, пробивало бы бюджет (≈570 МиБ, отказ записи до обрезки
	// — сценарий инцидента T-093). Потолок — последний рубеж памяти,
	// а не рабочий регулятор: стоячую глубину фолбэка в штатном режиме
	// ограничивают бюджет доли (T-239/ФТ-9) и темп воркера B (T-240),
	// неконтролируемый рост ловит алерт глубины (ТЗ 4.6 п.3, T-245);
	// усечённое неподтверждённое восстанавливается вне очереди, как и
	// у прочих потолков MAXLEN: штучные задачи — только перепроверкой
	// recheck (T-030, месяцы) — потому потолок и обязан быть запасным.
	defaultMaxLenRenderFallback = 15_000
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
	case StreamRenderFallback:
		return defaultMaxLenRenderFallback
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

// writeProbe — окно go-redis для пробы записи (T-152): SET короткоживущего
// ключа. Отдельный от claimStreams интерфейс: проба опциональна (клиенты
// боевых сборок умеют, фейки юнит-тестов — нет, Reliab.Writable тогда
// отвечает true и гейт клейма выключен — поведение до T-152).
type writeProbe interface {
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
}

// Параметры пробы записи (T-152).
const (
	// WriteProbeKey — ключ пробы доступности записи: SET с TTL, один
	// на инстанс Redis (значение не важно); TTL сам убирает ключ
	// остановившихся сервисов. noeviction-ответ на SET — тот же отказ,
	// что встретит XADD результата: проба честнее «прочитать ошибку
	// предыдущего шага» — не расходует ни попыток доставок, ни вызовов
	// обработчика.
	WriteProbeKey = "crawler:queue:write-probe"
	// writeProbeTTL — время жизни ключа пробы: с запасом больше
	// максимального redis.recover_interval (1 ч, конфигурация) — ключ
	// живёт, пока его обновляет живой RecoverLoop.
	writeProbeTTL = 2 * time.Hour
)

// claimRefunder — окно go-redis для возврата попыток необработанным
// записям (T-222): XCLAIM … JUSTID RETRYCOUNT через Do. Отдельный от
// claimStreams интерфейс по образцу writeProbe: типизированный XClaimArgs
// go-redis v9.23 не передаёт RETRYCOUNT (XClaim/XClaimJustID сериализуют
// только стрим/группу/потребителя/простои/ID), а семантика отката
// счётчика доставок — именно в RETRYCOUNT; *redis.Client (боевые сборки,
// queue.NewClient) и фейки юнит-тестов удовлетворяют интерфейсу
// (Reliab.Refund у клиента без Do прозрачен — поведение до T-222).
type claimRefunder interface {
	Do(ctx context.Context, args ...interface{}) *redis.Cmd
}

// ReliabOptions — параметры возврата зависших записей (T-025).
type ReliabOptions struct {
	// MaxAttempts — максимум доставок задачи (первая + клеймы) до
	// перевода в q:dead; 0 — DefaultMaxAttempts.
	MaxAttempts int
	// MinIdle — минимальный простой pending-записи до клейма, когда Claim
	// вызван с minIdle 0; 0 — DefaultClaimMinIdle.
	MinIdle time.Duration
	// OnBury — хук завершённого перевода записи в q:dead (T-154):
	// вызывается после успешной публикации Dead и XACK исходной записи —
	// ровно на факте смерти задачи, с именем стрима-источника.
	// Монтируют сервисы (счётчик смертей crawler_queue_dead_buried_total,
	// metrics.NewDeadBuriedHook): пакет очереди метрик не знает — стиль
	// queue-метрик кодовой базы (шов-функция, как LenFunc коллектора
	// глубин T-077). nil — перевод молчалив (как до T-154). Хук зовётся
	// только для записей с task_id; неидентифицируемые (без task_id,
	// терминальная ветка bury, T-260/ФТ-6.1) отмечаются OnBuryUnidentified.
	OnBury func(stream string)
	// OnBuryUnidentified — хук завершённого перевода в q:dead записи
	// без task_id (T-260, ФТ-6.1): Synthetic TaskID unknown:<stream>:
	// <entry-id>, без предметных полей. Отдельный от OnBury шов: смерть
	// задачи и карантин неидентифицируемой записи — разные события
	// (crawler_queue_dead_buried_total считает задачи с task_id,
	// crawler_queue_dead_unidentified_total — неидентифицируемые записи;
	// metrics.NewDeadUnidentifiedHook). nil — перевод молчалив.
	OnBuryUnidentified func(stream string)
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
	rdb    claimStreams // XAUTOCLAIM + XPENDING
	q      Queue        // публикация Dead и XACK исходных записей
	opt    ReliabOptions
	probe  writeProbe    // SET-проба записи (T-152); nil — проба недоступна, гейт выключен
	refund claimRefunder // XCLAIM JUSTID RETRYCOUNT (T-222); nil — возврат недоступен, отмена жжёт попытки как до T-222
}

// NewReliab строит слой надёжности поверх очереди q и клиента rdb — это
// должен быть один и тот же Redis (Dead публикуется и подтверждается через
// Queue, клейм и счётчик — напрямую): например NewReliab(rdb,
// NewRedisQueue(rdb, RedisOptions{}), opt). Нулевые значения опций
// заменяются значениями по умолчанию. Клиент, умеющий SET (боевые
// *redis.Client / *redis.ClusterClient), получает пробу записи
// (Writable, T-152).
func NewReliab(rdb claimStreams, q Queue, opt ReliabOptions) *Reliab {
	if opt.MaxAttempts <= 0 {
		opt.MaxAttempts = DefaultMaxAttempts
	}
	if opt.MinIdle <= 0 {
		opt.MinIdle = DefaultClaimMinIdle
	}
	r := &Reliab{rdb: rdb, q: q, opt: opt}
	if p, ok := rdb.(writeProbe); ok {
		r.probe = p
	}
	if rf, ok := rdb.(claimRefunder); ok {
		r.refund = rf
	}
	return r
}

// Writable — проба доступности Redis на запись (T-152): SET короткоживущего
// ключа WriteProbeKey. false — запись невозможна (OOM noeviction, обрыв
// соединения): клеймить зависшие доставки бессмысленно — обработка их
// вернётся той же ошибкой публикации, а каждый клейм расходует попытку
// доставки к q:dead (корень инцидента 22–23.10, §11.1). Проба стоит один
// SET и не трогает PEL. true также при неподдерживаемой пробе (клиент без
// SET — фейки юнит-тестов): гейт выключен, поведение до T-152.
func (r *Reliab) Writable(ctx context.Context) bool {
	if r.probe == nil {
		return true
	}
	return r.probe.Set(ctx, WriteProbeKey, 1, writeProbeTTL).Err() == nil
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
// доставку — первое чтение группы и каждый клейм; XAUTOCLAIM инкрементирует
// всей заклеймленной пачке сразу, в момент клейма, а не в момент начала
// обработки — необработанный при отмене хвост пачки получает попытку назад
// отдельной командой, Reliab.Refund, T-222). Тело записи стрима
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
// предпочтительнее потери (ТЗ 3.4). Запись без task_id (вне форматов
// пакета) переводится с синтетическим идентификатором — см. bury.
//
// Ошибка XPENDING одной записи не рушит пачку: XAUTOCLAIM уже закрепил
// записи за потребителем и сбросил им idle — потеря всей пачки на сбое
// одной проверки означала бы ещё один MinIdle простоя для записей,
// счётчик которых известен. Ошибки собираются в возврат (errors.Join),
// валидная часть отдаётся; запись с неизвестным счётчиком возвращается
// вторым результатом (unclassified): попытка её клеймом уже списана,
// RecoverLoop возвращает её возвратом RefundUnclassified (RETRYCOUNT
// откат, ФТ-6.2 — окно T-222 расширено с «отмена ctx» на «любая
// незавершённая классификация»); возврат не состоялся (окно Do
// недоступно, сбой команд) — запись остаётся закреплённой и вернётся
// следующим клеймом после нового MinIdle.
// Отмена ctx после успешного клейма чтения счётчиков не прерывает
// (WithoutCancel + собственный бюджет, T-222): прерывание оставило бы
// записи без классификации — их возврат потребовал бы повторного
// чтения счётчика (RefundUnclassified читает сам).
//
// Пустой результат без ошибки — зависших записей нет.
func (r *Reliab) Claim(ctx context.Context, stream, group, consumer string, minIdle time.Duration, n int, start string) ([]Claimed, []Delivery, string, error) {
	if !knownStream(stream) {
		return nil, nil, "", fmt.Errorf("queue: неизвестный стрим %q", stream)
	}
	if group == "" {
		return nil, nil, "", fmt.Errorf("queue: не задана группа потребителей стрима %s", stream)
	}
	if consumer == "" {
		return nil, nil, "", fmt.Errorf("queue: не задано имя потребителя группы %s", group)
	}
	if minIdle <= 0 {
		minIdle = r.opt.MinIdle
	}
	if n < 1 {
		return nil, nil, "", fmt.Errorf("queue: размер пачки клейма %d < 1", n)
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
		return nil, nil, "", fmt.Errorf("queue: claim %s (группа %s): %w", stream, group, err)
	}
	if next == "" {
		next = "0-0" // обход PEL завершён — следующий с начала
	}
	// Классификация счётчиков переживает отмену ctx вызывающего (T-222,
	// P2-2 ревью): XAUTOCLAIM выше уже закрепил записи и списал каждой
	// попытку — прерывать чтение счётчиков по отмене бессмысленно, а
	// записи без классификации возвращаются вторым результатом
	// (unclassified) и требуют повторного чтения счётчика при возврате
	// попытки (RefundUnclassified, ФТ-6.2). Поэтому после успешного
	// клейма чтения идут под отдельным ctx — без отмены вызывающего
	// (WithoutCancel), с собственным коротким бюджетом; сам XAUTOCLAIM
	// отмену уважает: не начинать новое изъятие из PEL после останова.
	cctx, cancelClassification := context.WithTimeout(context.WithoutCancel(ctx), classifyTimeout)
	defer cancelClassification()
	ctx = cctx
	out := make([]Claimed, 0, len(msgs))
	var unclassified []Delivery
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
			// Клейм уже списал попытку — запись возвращается для возврата
			// её (ФТ-6.2): без этого сбой чтения сжигал попытку бесследно.
			unclassified = append(unclassified, d)
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
	if len(out) == 0 && len(unclassified) == 0 {
		// Эквивалентные мутанты (T-M04): `> 0` → `>= 0`/`> -1` неотличимы —
		// errors.Join() без ошибок возвращает nil, ветка при пустых errs
		// совпадает с падением в return ниже. `> 1` (numbers/incrementer)
		// отличим одиночной ошибкой и не гасится.
		// mutator-disable-next-line expression/comparison, numbers/decrementer
		if len(errs) > 0 {
			return nil, nil, next, errors.Join(errs...)
		}
		return nil, nil, next, nil
	}
	return out, unclassified, next, errors.Join(errs...)
}

// Параметры возврата попыток необработанным записям (T-222).
const (
	// classifyTimeout — бюджет классификации пачки после клейма
	// (XPENDING по каждой записи, bury исчерпавших): чтения переживают
	// отмену ctx вызывающего (см. Claim), но не висят дольше бюджета
	// graceful shutdown; до Batch коротких чтений с запасом.
	classifyTimeout = 5 * time.Second
)

// refundKey — группа записей одного возврата попыток (Refund): RETRYCOUNT
// задаётся одним числом на команду XCLAIM, записи с разными счётчиками
// (и стримами) возвращаются отдельными командами.
type refundKey struct {
	stream   string
	attempts int
}

// Refund возвращает попытку доставки записям, заклеймленным XAUTOCLAIM,
// но не обработанным (T-222). Семантика счётчика (см. Claim): XAUTOCLAIM
// инкрементирует deliveries-счётчик PEL всей пачке в момент клейма — не
// в момент начала обработки, — поэтому отмена RecoverLoop посреди пачки
// (graceful shutdown: бюджет остановки 10 с меньше времени обхода пачки;
// рендер — до 25 с на запись) оставляла хвост пачки с сожжённой попыткой
// без единого вызова обработчика: минус попытка на каждом рестарте,
// после redis.max_attempts — q:dead без обработки. Гейт записи (T-152)
// закрывает соседний сценарий (недоступный Redis), отмену контекста он
// не видит. Вызывается из RecoverLoop для необработанного хвоста пачки
// под отдельным коротким контекстом — контекст цикла к этому моменту
// уже отменён. Записям, чья классификация не завершилась (сбой чтения
// XPENDING после клейма, Claim → unclassified), счётчик неизвестен —
// их возвращает RefundUnclassified (T-260/ФТ-6.2): окно T-222 расширено
// с «отмена ctx» на «любая незавершённая классификация».
//
// Механизм — XCLAIM <stream> <group> <consumer> 0 <ids…> JUSTID
// RETRYCOUNT <attempts-1> на того же потребителя, за которым записи
// закреплены клеймом:
//
//   - RETRYCOUNT устанавливает счётчик доставок точным значением
//     (семантика Redis, t_stream.c xclaimGenericCommand: RETRYCOUNT
//     перекрывает обычный инкремент клейма; JUSTID без RETRYCOUNT его
//     не трогает) — attempts-1 возвращает счётчик к значению до клейма:
//     следующая доставка увидит тот же номер попытки, что и отменённый
//     проход;
//   - JUSTID — без тел записей в ответе: тела не нужны, счётчик
//     возвращает явный RETRYCOUNT;
//   - min-idle 0 — записи закреплены за этим потребителем только что,
//     FORCE не нужен: записи в PEL группы. Переклейм RecoverLoop
//     соседнего воркера невозможен, пока обход пачки укладывается
//     в claim_min_idle (инвариант T-153) — RecoverLoop сверяет время
//     от клейма и вне инварианта возврат пропускает: возврат после
//     чужого клейма погасил бы и его инкремент счётчика;
//
// Побочный эффект: XCLAIM обновляет время доставки — простой
// возвращённых записей начинается заново, следующий клейм не раньше чем
// через claim_min_idle. Плата за честный счётчик — задержка возврата,
// не корректность (at-least-once, ТЗ 3.4).
//
// Записи группируются по (стрим, счётчику); ошибки команд собираются в
// возврат (errors.Join), сбой одной группы не отменяет остальные —
// вернуть хотя бы часть лучше, чем ничего (не возвращённая попытка —
// дожигание до q:dead, которого фикс избегает). Клиент без окна Do
// (nil) — прозрачный nil: попытки остаются расходованы, как до T-222.
func (r *Reliab) Refund(ctx context.Context, group, consumer string, claimed []Claimed) error {
	if r.refund == nil || len(claimed) == 0 {
		return nil
	}
	if group == "" || consumer == "" {
		return fmt.Errorf("queue: возврат попыток: не заданы группа (%q) или потребитель (%q)", group, consumer)
	}
	// Стрим — боевые константы пакета, как у Claim: XCLAIM по произвольному
	// имени не отправляем.
	for _, c := range claimed {
		if !knownStream(c.Stream) {
			return fmt.Errorf("queue: возврат попыток: неизвестный стрим %q", c.Stream)
		}
	}
	ids := make(map[refundKey][]string, len(claimed))
	for _, c := range claimed {
		n := c.Attempts - 1
		if n < 0 {
			n = 0 // защитный предел: счётчик доставок не бывает отрицательным
		}
		k := refundKey{stream: c.Stream, attempts: n}
		ids[k] = append(ids[k], c.ID)
	}
	var errs []error
	for k, list := range ids {
		args := make([]interface{}, 0, 8+len(list))
		args = append(args, "xclaim", k.stream, group, consumer, int64(0))
		for _, id := range list {
			args = append(args, id)
		}
		args = append(args, "justid", "retrycount", int64(k.attempts))
		if err := r.refund.Do(ctx, args...).Err(); err != nil {
			errs = append(errs, fmt.Errorf("queue: возврат попыток записям %s стрима %s (группа %s): %w",
				strings.Join(list, ","), k.stream, group, err))
		}
	}
	return errors.Join(errs...)
}

// RefundUnclassified возвращает попытку доставки записям, заклеймленным
// XAUTOCLAIM, но не получившим классификации — чтение счётчика XPENDING
// после клейма сбойнуло (T-260, ФТ-6.2; расширение окна T-222 с «отмена
// ctx» на «любая незавершённая классификация»). До фикса такая попытка
// сгорала бесследно: счётчик записям RecoverLoop уже не вернёт (Refund
// требует известный Attempts), а запись молча дожигалась до q:dead
// повторными клеймами без единого вызова обработчика.
//
// Счётчик читается в момент возврата (XPENDING EXTENDED по каждой
// записи, диапазон id..id — как классификация Claim): попытка клеймом
// уже списана, чтение без побочных эффектов, поэтому окно между сбоем
// классификации и возвратом не искажает значение. Запись, покинувшая
// PEL за это время (подтверждена тем, кто её обрабатывал, или похоронена
// соседом), возврата не требует — пропускается. Ошибка повторного
// чтения — в возврате (errors.Join), попытка записи остаётся
// израсходованной: не хуже поведения до T-260, следующая классификация
// решит её судьбу. Обработка записей не начиналась вовсе (классификация
// — до обработки), поэтому возврат правомерен для всей переданной
// пачки; записи с завершённой классификацией сюда не попадают — их
// возвращает Refund из необработанного хвоста (T-222).
//
// Сам возврат — тот же механизм Refund (XCLAIM JUSTID RETRYCOUNT
// <счётчик−1> на потребителя клейма): группировка, кламп нуля, окно Do
// (nil — прозрачный nil, попытки расходованы как до T-222) и инвариант
// T-153 (возврат гасит ровно свой инкремент — вызывается RecoverLoop
// немедленно после Claim) — см. Refund.
func (r *Reliab) RefundUnclassified(ctx context.Context, group, consumer string, ds []Delivery) error {
	if r.refund == nil || len(ds) == 0 {
		return nil
	}
	if group == "" || consumer == "" {
		return fmt.Errorf("queue: возврат попыток: не заданы группа (%q) или потребитель (%q)", group, consumer)
	}
	// Стрим — боевые константы пакета, как у Claim/Refund: XPENDING по
	// произвольному имени не отправляем.
	for _, d := range ds {
		if !knownStream(d.Stream) {
			return fmt.Errorf("queue: возврат попыток: неизвестный стрим %q", d.Stream)
		}
	}
	classified := make([]Claimed, 0, len(ds))
	var errs []error
	for _, d := range ds {
		pending, err := r.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: d.Stream,
			Group:  group,
			Start:  d.ID,
			End:    d.ID, // ровно эта запись: счётчик чтения ради, без разбора чужого pending
			Count:  1,
		}).Result()
		if err != nil {
			errs = append(errs, fmt.Errorf("queue: счётчик доставок записи %s (%s): %w", d.ID, d.Stream, err))
			continue
		}
		if len(pending) == 0 {
			// Запись покинула PEL между клеймом и возвратом — попытки
			// больше нет, возвращать нечего.
			continue
		}
		classified = append(classified, Claimed{Delivery: d, Attempts: int(pending[0].RetryCount)})
	}
	return errors.Join(append(errs, r.Refund(ctx, group, consumer, classified))...)
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
// Dead идёт до XACK (см. Claim). Завершённый перевод (Dead опубликован,
// исходная запись подтверждена) отмечается хуком OnBury — наблюдаемость
// смертей (T-154: bury был молчалив, 5507 смертей инцидента 22.10
// прошли без единого алерта).
//
// Запись без task_id (сообщение вне форматов пакета) получает
// терминальную ветку вместо ошибки (T-260, ФТ-6.1): до фикса bury
// возвращал ошибку, запись оставалась в PEL и крутилась вечным циклом
// клейма — каждый проход RecoverLoop клеймил её снова (+1 к счётчику)
// и упирался в ту же ошибку переноса, XACK не наступал никогда.
// Теперь Dead публикуется с синтетическим идентификатором
// unknownTaskID — unknown:<stream>:<entry-id>, без предметных полей
// (выжимка из тела, не разбирающегося как сообщение стрима, субъект
// задачи не идентифицирует), исходная запись подтверждается — запись
// покидает PEL, разбор остаётся ручным (q:dead — карантин). Перевод
// отмечается отдельным хуком OnBuryUnidentified (счётчик
// crawler_queue_dead_unidentified_total): смерть задачи и карантин
// неидентифицируемой записи — разные события. Записи с task_id
// обрабатываются как раньше.
func (r *Reliab) bury(ctx context.Context, group string, d Delivery, attempts int) error {
	taskID := taskIDOf(d.Payload)
	unidentified := taskID == ""
	if unidentified {
		taskID = unknownTaskID(d.Stream, d.ID)
	}
	reason := fmt.Sprintf("исчерпан лимит попыток %d: доставок без подтверждения — %d", r.opt.MaxAttempts, attempts)
	if unidentified {
		reason = fmt.Sprintf("запись вне форматов пакета (без task_id), лимит попыток %d: доставок без подтверждения — %d",
			r.opt.MaxAttempts, attempts)
	}
	dead := Dead{
		TaskID:   taskID,
		Stream:   d.Stream,
		EntryID:  d.ID,
		Attempts: attempts,
		Reason:   reason,
	}
	// Идентификаторы предмета (T-105) — только идентифицируемым записям:
	// выжимка из чужого тела может дать недопустимые значения (variant
	// вне 0/1, имя длиннее лимита) — Publish отклонил бы такую запись,
	// XACK не состоялся бы, и задача с task_id навсегда застряла бы
	// в pending. Тогда переносим базу без идентификаторов: q:dead —
	// карантин, обязан принять любую задачу с task_id (база всегда
	// валидна — проверены полями bury/Claim). Телу без task_id выжимка
	// не доверяется вовсе: предмет классифицировать нельзя (ФТ-6.1 —
	// запись без предметных полей).
	if !unidentified {
		subject := dead
		deadSubjectOf(&subject, d.Stream, d.Payload)
		if subject.validate() == nil {
			dead = subject
		}
	}
	if _, err := r.q.Publish(ctx, StreamDead, dead); err != nil {
		return fmt.Errorf("queue: публикация в q:dead записи %s стрима %s: %w", d.ID, d.Stream, err)
	}
	if err := r.q.Ack(ctx, d.Stream, group, d.ID); err != nil {
		return fmt.Errorf("queue: подтверждение перенесённой в q:dead записи %s стрима %s: %w", d.ID, d.Stream, err)
	}
	if unidentified {
		if r.opt.OnBuryUnidentified != nil {
			r.opt.OnBuryUnidentified(d.Stream)
		}
		return nil
	}
	if r.opt.OnBury != nil {
		r.opt.OnBury(d.Stream)
	}
	return nil
}

// unknownTaskID — синтетический идентификатор записи q:dead, из которой
// не извлекается task_id (T-260, ФТ-6.1): unknown:<stream>:<entry-id>.
// Идентифицирует самую запись (стрим-источник + идентификатор записи),
// а не задачу: связь с исходной задачей невозможна — тело вне форматов
// пакета. Префикс unknown отличает синтетику от настоящих task_id
// (hex NewTaskID) в ручном разборе q:dead; предметные поля записи
// остаются нулевыми.
func unknownTaskID(stream, entryID string) string {
	return "unknown:" + stream + ":" + entryID
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

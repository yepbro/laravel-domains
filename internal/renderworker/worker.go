// Package renderworker — этап рендера (T-067; ТЗ 5.6 «итоговый DOM заменяет
// сырой HTML в хранилище; в результате выставляется rendered = true», §5.7,
// §7 «render-worker → DOM + cookies → обновление результата → q:analyze»).
//
// Воркер потребляет q:render (сообщение Result — ключ строки
// subdomain_results: домен, субдомен, вариант; формат T-024, публикуют
// HTTP-этап T-066 при SPA-вердикте T-057 и проход доработки renderq.go).
// Сам результат остаётся в БД, сообщение только указывает на запись —
// итоговый URL для рендера читается из строки (сообщение несёт только
// идентификаторы, ТЗ 4.2). Обработка одной доставки:
//
//	SELECT строки (final_url, content_hash — цель рендера и старая ссылка)
//	→ render.Pool.Run (T-061…T-064: изолированный контекст, блокировка
//	  ресурсов, ожидание готовности, DOM + cookies + заголовки)
//	→ ContentStore.Put(DOM) — новая ссылка +1 (T-056/T-042, дедуп по хэшу)
//	→ ОДНА транзакция: UPDATE строки (rendered = true, новый content_hash,
//	  final_url/cookies/headers рендера, rules_version = NULL) +
//	  −1 refcount старого хэша (SwapRefTx, T-044)
//	→ после фиксации — публикация q:analyze (Result, тот же ключ; §7)
//	→ XACK (Processing, T-025: подтверждение только после записи в БД).
//
// Идемпотентность повторной доставки (ТЗ 3.4): перезапись без охраны
// rendered — повторный рендер перезаписывает контент и refcount (баланс
// в духе T-058: повторный Put даёт +1, UPDATE снимает старую ссылку).
// От гонки со свежей перепроверкой домена (recheck T-030 перезаписывает
// строки DELETE+INSERT, T-058) строка защищена оптимистической охраной
// UPDATE по content_hash: изменившаяся строка не перезаписывается (0
// строк — ссылка Put снимается, доставка подтверждается: задача была про
// прошлый эпизод, у нового свои публикация и рендер).
//
// Флаг render_pending (T-066) render-worker не трогает: владелец флага —
// HTTP-этап, его проход доработки снимает бит после публикации q:render;
// рендер меняет только строку результата (§7 — домен уже done, рендер
// асинхронное дополнение). Потребление q:render — скользящее (T-142,
// образец T-139): чтение при освобождении слота доставки, без барьера
// окна пачки.
//
// Политика таймаутов (T-148): таймаут страницы (render.timeout) —
// предсказуемый тарпит, а не случайная ошибка. Пока номер доставки
// (счётчик PEL, ProcessingAttempt/RecoverLoopAttempt) не достиг
// Options.TimeoutAttempts (render.timeout_attempts), тарпит живёт общим
// путём повторных доставок; на исчерпании запись финализируется без
// рендера: итог — сырой HTML HTTP-фазы (rendered = false), публикация
// q:analyze, XACK — работа не выпадает в q:dead, q:dead остаётся
// инфраструктурным ошибкам (БД, браузер, публикация; runbooks §2).
//
// Политика постоянных ошибок сертификата хоста (T-159, расширение T-172):
// ERR_CERT_AUTHORITY_INVALID / ERR_CERT_COMMON_NAME_INVALID — постоянные
// свойства хоста (сертификат не издан доверенным ЦС / не покрывает имя),
// ретрай бессмысленен; ERR_CERT_DATE_INVALID (просрочен/ещё не действует)
// добавлен по замеру T-172: успешных ретраев 0 из 321 первых неудач за
// сутки, 92% исчерпали попытки в q:dead — продление сертификата парковки
// на масштабе дней не успевает за 3 доставки (~полчаса). Cert-класс
// финализируется как тарпит, но с первой же доставки (без тарпит-лимита):
// итог — сырой HTML HTTP-фазы, публикация q:analyze, XACK; суточный
// прирост q:dead от cert-класса нулевой, очередь q:render не копит
// попытки cert-хостов.
//
// Политика последней попытки × класс задачи (T-172): прочие ошибки
// рендера класса task (отказ соединения, нерезолв, load error — свойства
// хоста, не инфраструктуры) живут общим путём повторных доставок, пока
// номер доставки ниже redis.max_attempts (Options.MaxAttempts; отказ
// соединения восстанавливается — замер T-172: 6.6% ретраев успешны), а
// на последней доставке запись финализируется без рендера по образцу
// тарпитов: итог — сырой HTML HTTP-фазы, публикация q:analyze, XACK.
// Смерти q:render остаются только инфраструктурным сбоям (БД,
// хранилище, публикация, браузер) — в духе T-148 «q:dead — настоящим
// инфраструктурным ошибкам».
package renderworker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"runtime/debug"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"

	"crawler/internal/content"
	"crawler/internal/db"
	"crawler/internal/metrics"
	"crawler/internal/queue"
	"crawler/internal/render"
)

// SQL этапа. rendered = true — литерал ТЗ 5.6; rules_version = NULL —
// контент сменился, прошлое заключение анализатора (rules_version ставит
// T-068+ при записи findings) новому контенту не соответствует: повторная
// публикация q:analyze ниже переанализирует запись и проставит свежую
// версию. Ключи — литералами типов (стиль record.go T-058).
var (
	// renderSelectSQL — цель рендера: итоговый URL строки результата и её
	// текущая ссылка на контент (оспариваемая оптимистической охраной
	// renderUpdateSQL). Ведущие колонки PK — партиционирование LIST по
	// zone_id и субпартиционирование HASH (T-005) режут поиск до листа.
	renderSelectSQL = "SELECT final_url, content_hash FROM subdomain_results " +
		"WHERE zone_id = $1::int2 AND domain_id = $2::int8 " +
		"AND subdomain_id = $3::int2 AND variant = $4::int2"

	// renderUpdateSQL — замена сырого HTML на итоговый DOM (ТЗ 5.6, §5.7):
	// rendered = true, content_hash — ссылка на DOM (Put выше уже дал ей
	// +1, здесь строка переключается на неё), final_url/cookies/headers —
	// данные рендера (JS-редиректи виден только рендеру — T-061; cookies —
	// все, включая httpOnly и выставленные JS, ТЗ 5.6/§11.4 п. 3 — единый
	// jsonb-контракт с T-058; заголовки — ответ итоговой навигации, ТЗ 5.8).
	// Охрана content_hash IS NOT DISTINCT FROM $9 — оптимистическая:
	// строка перезаписывается, только пока её контент тот же, что прочитан
	// (NULL сравнивается корректно — «контента нет» легально, T-056).
	renderUpdateSQL = "UPDATE subdomain_results AS r " +
		"SET rendered = true, final_url = $5::text, content_hash = $6::bytea, " +
		"cookies = $7::jsonb, headers = $8::jsonb, rules_version = NULL " +
		"WHERE r.zone_id = $1::int2 AND r.domain_id = $2::int8 " +
		"AND r.subdomain_id = $3::int2 AND r.variant = $4::int2 " +
		"AND r.content_hash IS NOT DISTINCT FROM $9::bytea"
)

// RenderDB — пул для чтения цели и записи результата: db.DB с
// транзакциями (стиль httpworker.RecordDB, content.RefDB). Замена строки
// и своп ссылок идут одной транзакцией (ТЗ 3.4); *pgxpool.Pool
// удовлетворяет интерфейсу.
type RenderDB interface {
	db.DB
	Begin(ctx context.Context) (pgx.Tx, error)
}

// ContentStore — шов хранилища контента (ТЗ 5.7): запись DOM и снятие
// ссылки, когда строка ушла из-под задачи. Узкое подмножество контракта
// content.ContentStore (Get этапу не нужен — DOM не читается обратно);
// удовлетворяется *content.PackStore (T-042).
type ContentStore interface {
	Put(ctx context.Context, data []byte) (content.PutResult, error)
	Delete(ctx context.Context, hash content.Hash) error
}

// Renderer — шов рендера: удовлетворяется *render.Manager (T-065 — боевая
// сборка cmd/render-worker: живое поколение «браузер + пул», перезапуски
// по страницам/памяти, реконнект и повтор задачи) и *render.Pool напрямую
// (T-061 — интеграционные тесты). Одна задача — одна страница в свежем
// изолированном контексте; параллелизм ограничен слотами самого пула
// (render.contexts, ТЗ 5.6).
type Renderer interface {
	Run(ctx context.Context, task render.RenderTask) (render.RenderOutput, error)
}

// Дефолты воркера.
const (
	// DefaultGroup — группа потребителей q:render (ТЗ 4.2: consumer groups
	// масштабируют воркеров горизонтально).
	DefaultGroup = "render-worker"

	// DefaultConcurrency — доставок q:render в работе одновременно.
	// Выше render.contexts не ускоряет сам рендер (слоты пула — узкое
	// горло этапа, ТЗ 5.6), но перекрывает его БД-часть (Put, транзакция,
	// публикация) — берём по прежней верхней границе рекомендованных
	// ТЗ контекстов (4; сама рекомендация ТЗ 5.6 — 2–4): значение —
	// пол для изолированных сборок и тестов без конфига. Дефолт
	// render.contexts с T-169 — 8, боевая сборка передаёт
	// max(DefaultConcurrency, render.contexts): слоты пула не должны
	// простаивать при contexts > 4 (cmd/render-worker).
	DefaultConcurrency = 4

	// DefaultConsume — записей q:render за одно чтение: размер пачки
	// XREADGROUP и буфера предвыборки скользящего потребления (T-142,
	// образец T-139) — чтение идёт вперёд обработки не глубже пары пачек
	// (текущая в руке читателя + буфер), следующее чтение происходит при
	// освобождении слотов доставок, окно чтения параллелизм не
	// ограничивает; столько же доставок подряд без подтверждения — порог
	// предохранителя паузы (renderFailWaitBase ниже).
	DefaultConsume = 16

	// DefaultTimeoutAttempts — попыток доставки, которые задача q:render
	// может завершить таймаутом страницы (render.timeout), прежде чем
	// запись финализируется без рендера (T-148): таймаут — предсказуемое
	// свойство тяжёлой страницы, а не случайная ошибка, жечь на нём общий
	// лимит redis.max_attempts (3 доставки → q:dead) — терять работу.
	// Вторая попытка после claim_min_idle (10 мин) даёт странице шанс
	// в изменившихся условиях (первая может таймаутнуться от затора —
	// замер 2026-10-23: слоты заняты таймаутами, 0.34 стр/с), дальше
	// повтор предсказуем. 2 из 3 доставок — тарпит-бюджет меньше общего,
	// q:dead остаётся инфраструктурным ошибкам (БД, браузер, публикация).
	DefaultTimeoutAttempts = 2

	// DefaultErrorWait — пауза после ошибок Redis и при недоступном
	// браузере (стиль httpworker.DefaultErrorWait): холостые круги не
	// греют логи, неподтверждённые доставки вернёт XAUTOCLAIM (T-025).
	DefaultErrorWait = 30 * time.Second
)

// Options — параметры воркера; нулевые значения заменяются дефолтами
// (withDefaults, стиль httpworker.Options).
type Options struct {
	// Concurrency — доставок q:render в работе одновременно; 0 —
	// DefaultConcurrency.
	Concurrency int
	// Consume — записей q:render за одно чтение; 0 — DefaultConsume.
	Consume int
	// Group — группа потребителей q:render; "" — DefaultGroup.
	Group string
	// Consumer — имя потребителя группы; "" — "render-worker-<pid>".
	Consumer string
	// Alive — зонд браузера: false — очередь не читается (доставки
	// остаются в стриме непрочитанными, а не в PEL без XACK — без
	// клеймов запись в PEL зависает надолго). Ставится сборкой сервиса
	// (render.Manager.Alive, T-061/T-065: мёртвое поколение зонд
	// перезапускает сам — реконнект); nil — зонд отключён.
	Alive func() bool
	// Recover — слой возврата зависших доставок (Reliab, T-025): Run
	// поднимает фоновый RecoverLoop (XAUTOCLAIM → та же обработка →
	// XACK) с теми же Group/Consumer; nil — восстановления нет
	// (изолированные сборки и тесты без Redis-специфики).
	Recover *queue.Reliab
	// TimeoutAttempts — сколько доставок задачи могут завершиться
	// таймаутом страницы (render.timeout, предсказуемый тарпит), прежде
	// чем запись финализируется без рендера — итогом остаётся сырой HTML
	// HTTP-фазы, запись уходит в q:analyze, доставка подтверждается
	// (T-148). Счётчик — номер доставки из PEL (ProcessingAttempt):
	// состояния в воркере нет, рестарты и горизонтальное масштабирование
	// семантику не меняют. Обязан быть ≤ redis.max_attempts — иначе
	// тарпит умер бы в q:dead до финализации (проверяет config.Validate).
	// 0 — DefaultTimeoutAttempts.
	TimeoutAttempts int
	// MaxAttempts — максимум доставок задачи (redis.max_attempts,
	// T-172): ошибка рендера класса task (отказ соединения, load error —
	// свойство хоста) на доставке с этим номером финализирует запись без
	// рендера (по образцу тарпитов T-148), а не уносит в q:dead; ниже
	// номера — обычные повторяемые ошибки (восстановление хостов
	// наблюдается: замер T-172 — 6.6% ретраев ERR_CONNECTION_REFUSED
	// успешны). Должен равняться redis.max_attempts того же Reliab
	// (боевая сборка передаёт cfg.Redis.MaxAttempts): меньше — записи
	// финализировались бы раньше исчерпания, больше — политика не
	// успевала бы до захоронения. 0 — queue.DefaultMaxAttempts.
	MaxAttempts int
	// RecoverBatch — пачка клейма RecoverLoop за проход; 0 — дефолт
	// очереди (queue.DefaultRecoverBatch). Боевая сборка передаёт
	// redis.recover_batch (T-153): темп возврата зависших — настраиваемая
	// величина очереди, а не окно потребления этапа. Инвариант с
	// claim_min_idle — queue.DefaultRecoverBatch (рендер — самый медленный
	// последовательный обход: 5–25 с/запись).
	RecoverBatch int
	// RecoverInterval — период опроса зависших записей RecoverLoop;
	// 0 — дефолт очереди (queue.DefaultRecoverInterval). Конфиг —
	// redis.recover_interval (T-153).
	RecoverInterval time.Duration
	// Log — логгер; nil — записи подавлены (slog.DiscardHandler).
	Log *slog.Logger
	// Metrics — реестр Prometheus сервиса (T-077, ТЗ 5.9): из него New
	// строит приборы этапа (NewMetrics); nil — без метрик (изолированные
	// сборки и тесты).
	Metrics *metrics.Registry
}

func (o Options) withDefaults() Options {
	if o.Concurrency <= 0 {
		o.Concurrency = DefaultConcurrency
	}
	if o.Consume <= 0 {
		o.Consume = DefaultConsume
	}
	if o.Group == "" {
		o.Group = DefaultGroup
	}
	if o.Consumer == "" {
		o.Consumer = fmt.Sprintf("render-worker-%d", os.Getpid())
	}
	if o.TimeoutAttempts <= 0 {
		o.TimeoutAttempts = DefaultTimeoutAttempts
	}
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = queue.DefaultMaxAttempts
	}
	return o
}

func (o Options) log() *slog.Logger {
	if o.Log == nil {
		return slog.New(slog.DiscardHandler)
	}
	return o.Log
}

// Worker — рендер-воркер: обрабатывает задачи q:render (ключи строк
// subdomain_results) через швы рендера и хранилища. Потокобезопасен:
// состояние неизменно после New.
type Worker struct {
	db       RenderDB
	q        queue.Queue
	contents ContentStore
	renderer Renderer
	opt      Options
	met      *Metrics // приборы этапа (T-077); nil — no-op
}

// New строит воркер поверх пула БД, очереди (публикация q:analyze — под
// гейтом backpressure в сборке, T-026), хранилища контента и пула
// рендера. nil-зависимости — ошибка: тихая потеря результата/публикации
// незаметна для оператора (стиль T-058).
func New(db RenderDB, q queue.Queue, contents ContentStore, renderer Renderer, opts Options) (*Worker, error) {
	if db == nil {
		return nil, errors.New("renderworker: пул БД не задан")
	}
	if q == nil {
		return nil, errors.New("renderworker: очередь не задана")
	}
	if contents == nil {
		return nil, errors.New("renderworker: хранилище контента не задано")
	}
	if renderer == nil {
		return nil, errors.New("renderworker: пул рендера не задан")
	}
	return &Worker{db: db, q: q, contents: contents, renderer: renderer, opt: opts.withDefaults(), met: NewMetrics(opts.Metrics)}, nil
}

// renderTarget — прочитанная строка результата: цель рендера и старая
// ссылка на контент.
type renderTarget struct {
	finalURL string
	hash     []byte // nil — контента нет (NULL content_hash, T-056)
}

// tarpitError — маркер таймаут-тарпита (T-148): ошибка рендера класса
// task, чья причина — истечение общего таймаута страницы render.timeout
// (session.wrap: «таймаут страницы …» → context.DeadlineExceeded в
// цепочке). Такой таймаут — предсказуемое свойство тяжёлой страницы:
// повтор на новом контексте почти наверняка даст тот же исход, а каждая
// попытка стоит слоту полный таймаут (замер 2026-10-23: слоты заняты
// таймаутами, эффективная скорость этапа 0.34 стр/с). Обёртка не меняет
// текст ошибки (Error/Unwrap — исходной) и служит только меткой:
// processDelivery выбирает по ней финализацию записи без рендера вместо
// пережигания попыток доставки до q:dead.
type tarpitError struct {
	err error
}

func (e *tarpitError) Error() string { return e.err.Error() }
func (e *tarpitError) Unwrap() error { return e.err }

// isPageTimeout — признак таймаута страницы: класс task (повтор на новом
// контексте бессмысленен) и context.DeadlineExceeded в цепочке — так
// помечает истечение render.timeout сам рендер (session.wrap). Отмена
// вызывающего контекста (graceful shutdown → context.Canceled) тарпитом
// не считается: это инфраструктурное событие, доставка вернётся
// повторной доставкой после рестарта.
func isPageTimeout(err error) bool {
	return render.ClassOf(err) == render.ClassTask && errors.Is(err, context.DeadlineExceeded)
}

// certErrorCodes — сетевые коды Chromium (net/base/net_error_list.h),
// означающие постоянную ошибку TLS-сертификата хоста: свойство самого
// хоста, а не сети или инфраструктуры. Список закрыт намеренно (T-159):
// ERR_CONNECTION_REFUSED и прочие сетевые коды не входят (отказ
// соединения может быть временным — хост поднимется; замер T-172: 6.6%
// ретраев после отказа успешны). Время-зависимый код сертификата
// ERR_CERT_DATE_INVALID (просрочен/ещё не действует) включён по замеру
// T-172: доля успешных ретраев 0 из 321 первых неудач за сутки (92%
// исчерпали попытки в q:dead) — окно продления сертификата (дни) много
// шире окна ретраев (3 доставки, ~полчаса), а следующий эпизод
// перепроверки (T-030) всё равно опубликует новую задачу q:render.
// Расширение списка — только с обоснованием детерминированности
// (замером) и тестом на код.
var certErrorCodes = []string{
	// Сертификат не издан доверенным центром сертификации: самоподписан
	// или неизвестный корень — замена сертификата на стороне хоста.
	"net::ERR_CERT_AUTHORITY_INVALID",
	// Сертификат не покрывает имя хоста (CN/SAN не совпадают) — ошибка
	// конфигурации сертификата хоста.
	"net::ERR_CERT_COMMON_NAME_INVALID",
	// Сертификат просрочен или ещё не действует. В T-159 был исключён
	// как «время-зависимый»; замер T-172 показал стабильность на окне
	// ретраев (0 успешных из 321) — включён.
	"net::ERR_CERT_DATE_INVALID",
}

// isCertError — признак постоянной ошибки сертификата хоста (T-159,
// T-172): класс task (свойство страницы — так classifyCtx маркирует сбои
// загрузки net::ERR_*, а не умерший контекст/браузер, где повтор осмыслен)
// и код ошибки загрузки из certErrorCodes. Сетевые коды (отказ
// соединения — восстановление наблюдается) мимо: до исчерпания
// детерминированности они — повторимые ошибки (T-172 финализирует их
// только на последней доставке).
func isCertError(err error) bool {
	if render.ClassOf(err) != render.ClassTask {
		return false
	}
	return slices.Contains(certErrorCodes, render.LoadErrorCode(err))
}

// certError — маркер постоянной ошибки сертификата хоста (T-159): ошибка
// рендера класса task, чья причина — отказ Chromium грузить страницу из-за
// сертификата хоста (isCertError: код net::ERR_CERT_* из certErrorCodes в
// цепочке). Свойство постоянное: сертификат не издан доверенным ЦС или не
// покрывает имя хоста — повтор на новом контексте (и вообще до смены
// сертификата самим хостом) даст тот же исход, а каждая попытка жгла бы
// слот рендера и общий лимит доставок redis.max_attempts до q:dead (замер
// §12: ~27% попыток рендера — cert-ошибки, каждая до 4 доставок; 366
// записей q:dead «исчерпан лимит попыток 3»). Обёртка не меняет текст
// ошибки (Error/Unwrap — исходной) и служит только меткой:
// processDelivery по ней финализирует запись без рендера с первой же
// доставки — ждать попыток, в отличие от тарпитов T-148, незачем.
type certError struct {
	err error
}

func (e *certError) Error() string { return e.err.Error() }
func (e *certError) Unwrap() error { return e.err }

// isTaskRenderError — признак ошибки рендера класса задачи (T-172):
// в цепочке есть *render.Error (сбой самого рендера, а не БД/хранилища/
// публикации — те *render.Error не несут, но render.ClassOf молча трактует
// посторонние ошибки как task, поэтому проверяется именно presence), класс
// task (свойство хоста: таймаут/load error — повтор на новом контексте
// бессмысленен) и это не отмена вызывающего контекста (graceful shutdown —
// инфраструктурное событие, как у isPageTimeout). Такие ошибки до
// исчерпания redis.max_attempts живут общим путём повторных доставок
// (восстановление хостов наблюдается), на последней — финализируются
// без рендера (processDelivery).
func isTaskRenderError(err error) bool {
	var re *render.Error
	if !errors.As(err, &re) {
		return false
	}
	return re.Class == render.ClassTask && !errors.Is(err, context.Canceled)
}

// taskFinalizeReason — метка причины финализации без рендера на последней
// попытке (T-172, метрика crawler_render_task_finalized_total): код
// ошибки загрузки net::ERR_* (структурно, render.LoadErrorCode — замкнутый
// словарь Chromium, кардинальность метки ограничена самим списком),
// «timeout» для таймаута, не пойманного тарпит-лимитом (TimeoutAttempts
// выше MaxAttempts — конфиг запрещает, страховка юнит-сборок), «other» —
// прочее (сбой страницы без кода загрузки).
func taskFinalizeReason(err error) string {
	if code := render.LoadErrorCode(err); code != "" {
		return code
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	return "other"
}

// ProcessResult обрабатывает одну задачу q:render (§5.6, §7): рендер
// итогового URL строки, DOM в хранилище, атомарная замена ссылки и
// rendered = true, публикация q:analyze после фиксации. Возвращает false
// без ошибки, когда задачи делать нечего: строки нет, у неё нет итогового
// URL или она ушла из-под задачи (перепроверка перезаписала — охрана
// UPDATE) — это успех, публикация не нужна, доставка подтверждается.
// Ошибка — сбой любого шага: доставка q:render не подтверждается и
// вернётся повторной доставкой (Processing, T-025); повторная обработка
// идемпотентна (перезапись, см. комментарий пакета).
func (w *Worker) ProcessResult(ctx context.Context, task queue.Result) (bool, error) {
	target, found, err := w.selectTarget(ctx, task)
	if err != nil {
		return false, err
	}
	if !found || target.finalURL == "" {
		// Строки нет (домен удалён/перепроверен без этого ключа) либо
		// итогового URL нет — рендерить нечего; штатный отказ без ошибки.
		w.opt.log().Debug("рендер не требуется — строки нет или без итогового URL",
			"zone_id", task.ZoneID, "domain_id", task.DomainID,
			"subdomain_id", task.SubdomainID, "variant", int16(task.Variant))
		return false, nil
	}
	name := renderLogName(task, target.finalURL)

	out, err := w.renderer.Run(ctx, render.RenderTask{URL: target.finalURL})
	if err != nil {
		// Ошибка страницы (таймаут, net::ERR_*) или браузера — повторная
		// доставка уровнем очереди (класс task) / ожидание или будущий
		// перезапуск (класс browser, T-065); частичного результата по ТЗ §8
		// не бывает (T-063). Постоянная ошибка сертификата хоста
		// маркируется certError (T-159), таймаут страницы — tarpitError
		// (T-148): processDelivery по маркерам решает — финализировать
		// запись без рендера или повторить доставкой.
		if isCertError(err) {
			return false, fmt.Errorf("renderworker: %s: рендер: %w", name, &certError{err: err})
		}
		if isPageTimeout(err) {
			return false, fmt.Errorf("renderworker: %s: рендер: %w", name, &tarpitError{err: err})
		}
		return false, fmt.Errorf("renderworker: %s: рендер: %w", name, err)
	}
	if out.DOM == "" || out.FinalURL == "" {
		// Страховка контракта T-064: живая страница всегда имеет
		// documentElement и location; пустые значения — ненормальный
		// результат, строку не трогаем.
		return false, fmt.Errorf("renderworker: %s: пустой результат рендера (DOM %d байт, final_url %q)",
			name, len(out.DOM), out.FinalURL)
	}
	// DOM — одной копией байтов (R-02): хэш и Put из одного среза, сам
	// итоговый DOM уже ограничен рендером (render.Options.MaxDOMBytes);
	// кратные полные копии строки не нужны.
	dom := []byte(out.DOM)
	newHash := content.HashOf(dom)

	var oldHash content.Hash
	hasOld := len(target.hash) == content.HashLen
	if hasOld {
		copy(oldHash[:], target.hash)
	}

	// Put до транзакции (порядок T-056 → T-058: новая ссылка обязана
	// существовать до свопа, SwapRefTx требует живой newHash). Контент не
	// сменился (DOM побайтово равен сохранённому) — Put пропускается:
	// ссылка у строки уже есть, лишний +1 не нужен; охрана UPDATE ниже
	// сравнивает с тем же хэшем и проходит.
	// Ошибки: ErrStoreFull — временное (повтор доставки дождётся места),
	// ErrTooLarge — постоянное свойство страницы (DOM раздут JS): попытки
	// доставки исчерпаются и задача уйдёт в q:dead на ручной разбор
	// (T-025) — оператору сигнал поднять content.max_store_bytes.
	if !hasOld || newHash != oldHash {
		if _, err := w.contents.Put(ctx, dom); err != nil {
			return false, fmt.Errorf("renderworker: %s: сохранение DOM: %w", name, err)
		}
	}
	cookies, err := renderCookiesJSON(out.Cookies)
	if err != nil {
		return false, fmt.Errorf("renderworker: %s: cookies: %w", name, err)
	}
	headers, err := renderHeadersJSON(out.ResponseHeaders)
	if err != nil {
		return false, fmt.Errorf("renderworker: %s: заголовки: %w", name, err)
	}

	tx, err := w.db.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("renderworker: %s: замена контента: транзакция: %w", name, err)
	}
	// Откат безусловно: после Commit — no-op (стиль T-044/T-058).
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx, renderUpdateSQL,
		task.ZoneID, task.DomainID, task.SubdomainID, int16(task.Variant),
		out.FinalURL, newHash[:], cookies, headers, target.hash)
	if err != nil {
		return false, fmt.Errorf("renderworker: %s: замена контента: %w", name, err)
	}
	if tag.RowsAffected() == 0 {
		// Оптимистическая охрана: контент строки сменился после чтения
		// (перепроверка домена перезаписала результат T-058, конкурентная
		// доставка уже записала свой DOM) — строка не наша, транзакция
		// откатывается отложенным Rollback. Ссылка Put снимается: без
		// строки она повисла бы утечкой места (контент-то жив пока есть
		// ссылки; Delete снимает ровно добавленную Put). Ошибка снятия —
		// не ошибка задачи: Warn, доставка подтверждается.
		_ = tx.Rollback(ctx)
		if !hasOld || newHash != oldHash {
			if err := w.contents.Delete(ctx, newHash); err != nil && !errors.Is(err, content.ErrNotFound) {
				w.opt.log().Warn("рендер: ссылка контента не снята (строка ушла из-под задачи)",
					"domain", name, "hash", newHash.String(), "err", err)
			}
		}
		w.opt.log().Debug("рендер не записан — строка перезаписана другим эпизодом",
			"domain", name, "zone_id", task.ZoneID, "domain_id", task.DomainID,
			"subdomain_id", task.SubdomainID, "variant", int16(task.Variant))
		return false, nil
	}

	// −1 старому контенту (ТЗ 5.7 «при перезаписи результата счётчик
	// ссылок старого контента уменьшается»): SwapRefTx(old, нулевой) —
	// примитив «только −старому», новый уже имеет +1 от Put (T-058).
	// Алиасные строки (hash NULL, T-055) попадают в hasOld=false — снимать
	// нечего; DOM алиаса — его собственная ссылка.
	if hasOld && newHash != oldHash {
		if err := content.SwapRefTx(ctx, tx, oldHash, content.Hash{}); err != nil {
			return false, fmt.Errorf("renderworker: %s: %w", name, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		// Откат вернёт строку; +1 от Put останется утечкой места —
		// безопасная сторона (T-044), повторная доставка перезапишет.
		return false, fmt.Errorf("renderworker: %s: замена контента: фиксация: %w", name, err)
	}

	// Публикация q:analyze после записи (§7): Result указывает на строку —
	// она обязана существовать к моменту потребления анализатором в новом
	// контенте. Сбой публикации → доставка q:render без XACK: повторная
	// доставка перезапишет результат идемпотентно и опубликует заново
	// (at-least-once, ТЗ 3.4; дубликаты ограничены попытками доставки).
	// Fresh: рендер дорабатывает запись текущего HTTP-эпизода (T-066),
	// а не переанализ — история детекции пишётся (T-118).
	task.Fresh = true
	if _, err := w.q.Publish(ctx, queue.StreamAnalyze, task); err != nil {
		return true, fmt.Errorf("renderworker: %s: публикация q:analyze: %w", name, err)
	}
	w.opt.log().Debug("DOM записан",
		"domain", name, "final_url", out.FinalURL, "dom_bytes", len(out.DOM),
		"cookies", len(out.Cookies))
	return true, nil
}

// selectTarget читает цель рендера по ключу задачи. SELECT вне транзакции:
// конкурентная перезапись строки между чтением и UPDATE ловится охраной
// renderUpdateSQL, повторное чтение даёт повторную попытку с актуальным
// URL (идемпотентность доставки).
func (w *Worker) selectTarget(ctx context.Context, task queue.Result) (renderTarget, bool, error) {
	rows, err := w.db.Query(ctx, renderSelectSQL,
		task.ZoneID, task.DomainID, task.SubdomainID, int16(task.Variant))
	if err != nil {
		return renderTarget{}, false, fmt.Errorf("renderworker: чтение строки результата: %w", err)
	}
	defer rows.Close()

	var t renderTarget
	if rows.Next() {
		var finalURL *string
		if err := rows.Scan(&finalURL, &t.hash); err != nil {
			return renderTarget{}, false, fmt.Errorf("renderworker: чтение строки результата: %w", err)
		}
		if finalURL != nil {
			t.finalURL = *finalURL
		}
		return t, true, nil // PK — не больше одной строки
	}
	if err := rows.Err(); err != nil {
		return renderTarget{}, false, fmt.Errorf("renderworker: чтение строки результата: %w", err)
	}
	return renderTarget{}, false, nil
}

// renderLogName — имя для логов: имя домена из сообщения, иначе ключ
// строки с целевым URL (Name опционально, T-024).
func renderLogName(task queue.Result, finalURL string) string {
	if task.Name != "" {
		return task.Name
	}
	return fmt.Sprintf("%d/%d/%d/%d → %s", task.ZoneID, task.DomainID, task.SubdomainID, int16(task.Variant), finalURL)
}

// renderCookiesJSON сериализует cookies рендера в jsonb (единый контракт
// subdomain_results.cookies: ExtractCookie повторяет запись HTTP-этапа,
// T-064/T-058); пустой набор — NULL.
func renderCookiesJSON(cookies []render.ExtractCookie) (*string, error) {
	if len(cookies) == 0 {
		return nil, nil
	}
	b, err := json.Marshal(cookies)
	if err != nil {
		return nil, err
	}
	s := string(b)
	return &s, nil
}

// renderHeadersJSON сериализует заголовки итоговой навигации в jsonb
// (канонические имена http.Header, как у T-058); пустой набор — NULL.
func renderHeadersJSON(h http.Header) (*string, error) {
	if len(h) == 0 {
		return nil, nil
	}
	b, err := json.Marshal(h)
	if err != nil {
		return nil, err
	}
	s := string(b)
	return &s, nil
}

// --- рабочий цикл ------------------------------------------------------------

// renderFailWaitBase — стартовая пауза предохранителя рабочего цикла
// (Run): доставки, подряд оставшиеся без XACK числом в целую пачку чтения
// (Consume) при систематическом сбое обработки — рендер/БД/публикация, —
// приостанавливают чтение q:render с удвоением паузы до DefaultErrorWait
// (образец — workerFailWaitBase httpworker, T-139). Без паузы цикл
// продолжал бы черпать задачи: каждая доставка жгла попытку (XAUTOCLAIM
// после DefaultClaimMinIdle, T-025) и уносила её в q:dead. Переменная, а
// не константа: юнит-тесты сжимают паузу, не выжидая секунды.
var renderFailWaitBase = time.Second

// renderFailBackoff — пауза после streak подряд выдержанных пауз
// предохранителя (серия нарастает, пока читатель не увидит ни одного
// успеха — счётчик неудач обнуляется подтверждённой доставкой):
// удвоение от renderFailWaitBase, потолок DefaultErrorWait (сдвиг
// ограничен — переполнение Duration). streak < 1 ограничивается до 1 —
// отрицательный сдвиг Duration паникует (ревью T-142).
func renderFailBackoff(streak int) time.Duration {
	if streak < 1 {
		streak = 1
	}
	d := renderFailWaitBase << min(streak-1, 5)
	if d > DefaultErrorWait {
		d = DefaultErrorWait
	}
	return d
}

// Run — рабочий цикл render-worker (ТЗ 4.2, §5.6, §7): читает задачи
// q:render группой потребителей пачками Consume и обрабатывает доставки
// параллельно до Concurrency, каждую — паттерном Processing (T-025):
// рендер → запись в БД → публикация q:analyze → XACK. Потребление
// скользящее (T-142, образец T-139 httpworker): следующее чтение
// происходит при освобождении слотов доставок (чтение идёт вперёд
// обработки не глубже пары пачек Consume), цикл не ждёт завершения
// пачки целиком — барьер окна на хвосте длительностей (страницы до
// render.timeout, 20–25 с) держал бы слоты праздными и резал потолок
// этапа поверх узкого горла Chromium (замер T-092 §4: 5.1 с/страница).
// Живость браузера (Options.Alive) проверяется перед каждым чтением:
// недоступный Chromium — пауза потребления, доставки остаются в стриме
// непрочитанными (не в PEL), перезапуск браузера — T-065. Работает до
// отмены ctx и возвращает nil — контракт OnRun каркаса (T-012; начатые
// задачи дорабатывают или возвращаются очереди повторной доставкой,
// ТЗ 3.4). Ошибка чтения или обработки не останавливает цикл:
// логируется (Warn), неподтверждённая доставка вернётся XAUTOCLAIM
// (ТЗ 4.2); Consume доставок подряд без единого подтверждения —
// систематический сбой обработки — выдерживает паузу-предохранитель
// (renderFailWaitBase): черпать новые задачи при лежащей обработке
// бессмысленно, попытки доставок сгорают. Инфраструктурный сбой
// публикации (InfraError: OOM Redis/соединение, T-152) останавливает
// чтение сразу и до восстановления записи (Reliab.Writable).
func Run(ctx context.Context, w *Worker, opts Options) error {
	if w == nil {
		return errors.New("renderworker: цикл воркера: воркер не задан")
	}
	o := opts.withDefaults()
	log := o.log()
	log.Info("цикл render-worker",
		"group", o.Group, "consumer", o.Consumer,
		"concurrency", o.Concurrency, "consume", o.Consume, "alive", o.Alive != nil)

	wait := func(d time.Duration) {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-ctx.Done():
		case <-t.C:
		}
	}

	// Возврат зависших доставок (T-025): фоновый RecoverLoop от имени
	// того же потребителя — клейм XAUTOCLAIM и та же обработка задач.
	// RecoverLoopAttempt (T-148): обработчик получает номер доставки
	// (Claimed.Attempts) для политики таймаутов. Живёт до отмены ctx;
	// Run дожидается его выхода (начатая задача дорабатывает или остаётся
	// неподтверждённой, ТЗ 3.4).
	if o.Recover != nil {
		recDone := make(chan struct{})
		go func() {
			defer close(recDone)
			_ = queue.RecoverLoopAttempt(ctx, o.Recover, w.processDelivery, queue.RecoverOptions{
				Stream:   queue.StreamRender,
				Group:    o.Group,
				Consumer: o.Consumer,
				// Темп обхода PEL — очередь, не этап (redis.recover_*,
				// T-153); нули — дефолты очереди.
				Batch:    o.RecoverBatch,
				Interval: o.RecoverInterval,
				Log:      log,
			})
		}()
		defer func() { <-recDone }()
	}

	sem := make(chan struct{}, o.Concurrency)
	// Скользящее потребление (T-142, образец T-139): читатель держит
	// впереди обработки не больше пары пачек Consume доставок (текущая
	// пачка в руке + буфер pending), диспетчер ниже занимает слот
	// семафора на время одной доставки и сразу берёт следующую —
	// завершение задачи освобождает слот и двигает чтение, никто не
	// ждёт пачку целиком. Прочитанные, но не начатые доставки (буфер
	// полон, слоты заняты хвостом длительностей) на отмене ctx остаются
	// без XACK и возвращаются очереди клеймом XAUTOCLAIM — как и не
	// начатые доставки окна прежде (at-least-once, ТЗ 3.4).
	//
	// Окно удержания доставок живым воркером (прочитано и не
	// подтверждено) — до 2×Consume + Concurrency ≈ 40 записей: оно
	// обязано укладываться в redis.claim_min_idle с запасом (иначе
	// клейм перехватывает живую работу); при дефолтных contexts=8
	// (Concurrency 8) и render.timeout=25 с хвост ожидает ≤ ~2,1 мин
	// против порога 10 мин, при Concurrency 1 окно растёт до ~13,8 мин —
	// поднимайте claim_min_idle вместе с понижением параллелизма
	// (ревью T-142; числа пересчитаны под дефолт 8, T-169).
	pending := make(chan queue.Delivery, o.Consume)
	var failed atomic.Int64 // доставок подряд без подтверждения
	// infraDown — устойчивая инфраструктурная ошибка публикации
	// (OOM/соединение Redis, T-152): чтение q:render приостанавливается
	// до восстановления записи (Reliab.Writable) — публикации всё равно
	// падают, новые доставки только копились бы в pending.
	var infraDown atomic.Bool
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		failStreak := 0  // подряд выдержанных пауз предохранителя
		infraStreak := 0 // подряд выдержанных пауз инфраструктуры
		for {
			// Живость браузера — перед каждым чтением: мёртвый Chromium
			// задачи не принимает, новые чтения прекращаются — не
			// прочитанные доставки остаются в стриме (не в PEL — клеймы
			// не жгут попытки, T-025); уже прочитанные в буфер
			// предвыборки дорабатываются диспетчером и при неудаче
			// осядут в PEL до клейма — как и окно прежнего барьерного
			// цикла, поведение не хуже. Поколение зонды менеджера
			// заменяют сами (реконнект, T-065).
			if o.Alive != nil && !o.Alive() {
				log.Warn("Chromium недоступен — q:render не читается (перезапуск — менеджер, T-065)")
				wait(DefaultErrorWait)
				if ctx.Err() != nil {
					return // graceful shutdown
				}
				continue
			}
			// Пауза потребления при устойчивой InfraError публикации
			// (T-152): пока проба записи падает — чтение не ведётся
			// (предохранитель ниже считал бы те же неудачи дольше и
			// читал бы пачки между паузами). Проба — один SET; после
			// восстановления записи чтение возобновляется немедленно.
			if infraDown.Load() {
				if o.Recover != nil && !o.Recover.Writable(ctx) {
					infraStreak++
					pause := renderFailBackoff(infraStreak)
					log.Warn("пауза чтения q:render: публикация результатов падает об инфраструктуру Redis (OOM/соединение) — новые доставки не читаются (T-152)",
						"streak", infraStreak, "wait", pause.String())
					wait(pause)
					if ctx.Err() != nil {
						return // graceful shutdown: wait вышел по отмене
					}
					continue
				}
				infraDown.Store(false)
				infraStreak = 0
				if o.Recover != nil {
					log.Info("запись в Redis восстановлена — чтение q:render возобновляется")
				}
			}
			// Предохранитель (renderFailWaitBase): доставки, подряд
			// оставшиеся без подтверждения числом в целую пачку чтения
			// (Consume) без единого успеха — систематический сбой
			// обработки (рендер/БД/публикация); пауза с удвоением до
			// DefaultErrorWait. Подтверждённая доставка обнуляет счётчик
			// неудач сразу, серия пауз сбрасывается на следующей
			// итерации читателя, увидевшей счётчик нулевым.
			if n := failed.Load(); n == 0 {
				failStreak = 0
			} else if int(n) >= o.Consume {
				if ctx.Err() != nil {
					return // graceful shutdown: паузы не было и не будет
				}
				failStreak++
				pause := renderFailBackoff(failStreak)
				// Наблюдаемость простоя: пауза останавливает чтение
				// этапа — Warn с серией и числом неудач (частота
				// ограничена удвоением паузы, не чаще ~2/мин на серии).
				log.Warn("пауза предохранителя чтения q:render: доставки падают подряд",
					"failed", n, "streak", failStreak, "wait", pause.String())
				wait(pause)
				if ctx.Err() != nil {
					return // graceful shutdown: wait вышел по отмене
				}
			}
			deliveries, err := w.q.Consume(ctx, queue.StreamRender, o.Group, o.Consumer, o.Consume)
			if err != nil {
				if ctx.Err() != nil {
					return // graceful shutdown
				}
				log.Warn("цикл render-worker: чтение q:render", "err", err)
				wait(DefaultErrorWait)
				continue
			}
			for _, d := range deliveries {
				select {
				case pending <- d:
				case <-ctx.Done():
					return // буферные доставки без XACK вернёт XAUTOCLAIM
				}
			}
		}
	}()

	// Диспетчер: ждёт свободный слот (или отмену) и запускает доставку.
	// Слот семафора — с выходом по отмене: отмена ctx во время ожидания
	// слота не должна запускать доставку с уже отменённым ctx (выбор
	// канала select недетерминирован, поэтому отмена проверяется и после
	// получения слота).
	var wg sync.WaitGroup
dispatch:
	for {
		var d queue.Delivery
		select {
		case <-ctx.Done():
			break dispatch // доставки буфера не начаты — без XACK
		case d = <-pending:
		}
		select {
		case <-ctx.Done():
			break dispatch
		case sem <- struct{}{}:
		}
		if ctx.Err() != nil {
			<-sem
			break dispatch
		}
		wg.Add(1)
		go func(d queue.Delivery) {
			defer wg.Done()
			defer func() { <-sem }()
			// Паника доставки не должна ронять процесс без лога и
			// очистки — тот же инвариант, что у горутины OnRun
			// (internal/service): panic → Error-лог, доставка без
			// XACK вернётся повторной доставкой (XAUTOCLAIM, T-025);
			// in-flight соседей и graceful shutdown сохраняются.
			// Для предохранителя паника — та же неподтверждённая
			// доставка: серия паник пережигала бы попытки без паузы.
			defer func() {
				if r := recover(); r != nil {
					failed.Add(1)
					log.Error("паника обработки доставки q:render",
						"entry", d.ID, "panic", r, "stack", string(debug.Stack()))
				}
			}()
			// Свежая доставка XREADGROUP — первая по счётчику PEL: attempts=1
			// (T-148, политика таймаутов в processDelivery).
			err := queue.ProcessingAttempt(ctx, w.q, o.Group, d, 1, w.processDelivery)
			if err != nil {
				failed.Add(1)
				if queue.IsInfra(err) {
					// Инфраструктурный сбой публикации (T-152):
					// чтение останавливает ветка infraDown читателя.
					infraDown.Store(true)
				}
				if ctx.Err() == nil {
					log.Warn("задача q:render не подтверждена — вернётся повторной доставкой",
						"entry", d.ID, "err", err)
				}
			} else {
				failed.Store(0)
				// Успех означает состоявшуюся публикацию результата —
				// запись Redis доступна, пауза снимается досрочно.
				infraDown.Store(false)
			}
		}(d)
	}
	wg.Wait()    // начатые доставки дорабатывают (graceful stop, ТЗ 3.4)
	<-readerDone // читатель завершается той же отменой ctx (Consume/wait)
	return nil
}

// processDelivery обрабатывает одну доставку q:render: декодирование
// Result-ключа и замена контента. attempts — номер доставки по счётчику
// PEL (1 — свежая XREADGROUP, Claimed.Attempts — клейм; ProcessingAttempt/
// RecoverLoopAttempt, T-148). Подтверждение (XACK) выполняет Processing
// после nil-возврата — «XACK только после записи результата в БД»
// (ТЗ 4.2); ядовитая запись (вне формата T-024) возвращается ошибкой —
// очередь исчерпает попытки и унесёт её в q:dead (T-025).
//
// Политика таймаутов (T-148): таймаут страницы (render.timeout,
// tarpitError) — предсказуемый тарпит, а не случайная ошибка: пока номер
// доставки не достиг TimeoutAttempts, доставка возвращается ошибкой как
// любая повторимая (без XACK → клейм → новая попытка), на исчерпании —
// запись финализируется без рендера (finalizeSkipRender) и доставка
// подтверждается: работа не выпадает из конвейера в q:dead, q:dead
// остаётся настоящим инфраструктурным ошибкам (БД, браузер, публикация).
// Аппроксимация: номер доставки ≥ числа таймаутов этой задачи — смешанная
// последовательность (инфраструктурный сбой, затем таймаут) финализирует
// на одну попытку раньше; для настоящих тарпитов (таймаут на каждой
// доставке) счёт совпадает точно.
//
// Постоянные ошибки сертификата хоста (T-159, certError) финализируются
// с первой же доставки — независимо от номера и тарпит-лимита: свойство
// хоста не меняется между попытками, ожидание бессмысленно.
//
// Политика последней попытки × класс задачи (T-172): прочая ошибка
// рендера класса task (isTaskRenderError — отказ соединения, load error,
// свойство хоста) на доставке с номером redis.max_attempts
// (Options.MaxAttempts) финализируется по образцу тарпитов вместо
// захоронения — на ранних попытках ошибка повторяема (восстановление
// хостов наблюдается), исчерпавший попытки хост получает честный сырой
// HTML HTTP-фазы и анализ. Инфраструктурный класс (БД, хранилище,
// публикация, браузер, отмена ctx) умирает прежним путём — q:dead
// остаётся настоящим инфраструктурным ошибкам.
//
// Успешная обработка идёт в приборы этапа (T-077): счётчик задач и
// длительность, rendered=true при записанном DOM; ошибка — в счётчик
// классов рендера (доставка вернётся повторной доставкой, ТЗ 3.4),
// финализация без рендера — в crawler_render_tarpit_finalized_total /
// crawler_render_cert_finalized_total /
// crawler_render_task_finalized_total{reason}.
func (w *Worker) processDelivery(ctx context.Context, del queue.Delivery, attempts int) error {
	task, err := queue.Decode[queue.Result](del)
	if err != nil {
		return fmt.Errorf("renderworker: запись %s стрима %s: %w", del.ID, del.Stream, err)
	}
	start := time.Now()
	rendered, err := w.ProcessResult(ctx, task)
	if err != nil {
		var ce *certError
		if errors.As(err, &ce) {
			// Cert-класс финализируется с первой же доставки (T-159): ошибка
			// сертификата — постоянное свойство хоста, ожидание попыток
			// (тарпит-лимит T-148 даёт странице шанс в изменившихся условиях —
			// здесь шанса нет) только жгло бы доставки к q:dead. Итог — сырой
			// HTML HTTP-фазы, конвейер продолжается анализом (q:analyze),
			// доставка подтверждается.
			if ferr := w.finalizeCertError(ctx, task, ce); ferr != nil {
				return fmt.Errorf("renderworker: домен %s: %w", task.Name, ferr)
			}
			w.met.Task(time.Since(start))
			w.met.Error(render.ClassTask)
			w.met.CertFinalized()
			return nil
		}
		var tp *tarpitError
		if errors.As(err, &tp) && attempts >= w.opt.TimeoutAttempts {
			// Лимит таймаутов исчерпан (T-148): запись финализируется без
			// рендера — итог сырой HTML HTTP-фазы, конвейер продолжается
			// анализом (q:analyze), доставка подтверждается.
			if ferr := w.finalizeSkipRender(ctx, task, tp, attempts); ferr != nil {
				return fmt.Errorf("renderworker: домен %s: %w", task.Name, ferr)
			}
			w.met.Task(time.Since(start))
			w.met.Error(render.ClassTask)
			w.met.TarpitFinalized()
			return nil
		}
		if attempts >= w.opt.MaxAttempts && isTaskRenderError(err) {
			// Последняя попытка × класс задачи (T-172): ошибка — свойство
			// хоста (отказ соединения, load error), дальше только q:dead;
			// ретраи уже даны (восстановление наблюдается — замер T-172:
			// 6.6% ретраев ERR_CONNECTION_REFUSED успешны), работу не
			// теряем: итог — сырой HTML HTTP-фазы, конвейер продолжается
			// анализом (q:analyze), доставка подтверждается.
			if ferr := w.finalizeTaskError(ctx, task, err, attempts); ferr != nil {
				return fmt.Errorf("renderworker: домен %s: %w", task.Name, ferr)
			}
			w.met.Task(time.Since(start))
			w.met.Error(render.ClassTask)
			w.met.TaskFinalized(taskFinalizeReason(err))
			return nil
		}
		w.met.Error(render.ClassOf(err))
		return fmt.Errorf("renderworker: домен %s: замена контента: %w", task.Name, err)
	}
	w.met.Task(time.Since(start))
	if rendered {
		w.met.Rendered()
	}
	return nil
}

// finalizeNoRender — общая механика финализации записи без рендера
// (T-148/T-159/T-172): строки нет (перепроверка перезаписала) или у неё
// нет итогового URL — штатный отказ без публикации; иначе запись уходит
// в q:analyze тем же ключом (Fresh=true — доработка текущего эпизода,
// как у успешного рендера) и Warn-лог с деталями причины (msg и
// пары «ключ-значение» вызывающей политики). Публикация идёт до XACK:
// сбой — ошибка, доставка вернётся и финализация повторится (дубликат
// публикации ограничен попытками, анализатор идемпотентен по ключу —
// тот же паттерн, что у рендера после сбоя публикации). Уже
// отрендеренная конкурентной доставкой строка анализируется повторно —
// дубль анализа тем же контентом безвреден (at-least-once, ТЗ 3.4).
func (w *Worker) finalizeNoRender(ctx context.Context, task queue.Result, msg string, fields ...any) error {
	target, found, err := w.selectTarget(ctx, task)
	if err != nil {
		return fmt.Errorf("финализация без рендера: чтение строки результата: %w", err)
	}
	if !found || target.finalURL == "" {
		// Рендерить/анализировать нечего: строки нет или она ушла из-под
		// задачи (перепроверка) — успех без публикации.
		w.opt.log().Debug("финализация без рендера не требуется — строки нет или без итогового URL",
			"zone_id", task.ZoneID, "domain_id", task.DomainID,
			"subdomain_id", task.SubdomainID, "variant", int16(task.Variant))
		return nil
	}
	task.Fresh = true
	if _, err := w.q.Publish(ctx, queue.StreamAnalyze, task); err != nil {
		return fmt.Errorf("финализация без рендера: публикация q:analyze: %w", err)
	}
	args := append([]any{"domain", renderLogName(task, target.finalURL), "final_url", target.finalURL}, fields...)
	w.opt.log().Warn(msg, args...)
	return nil
}

// finalizeSkipRender — финализация записи без рендера по тарпиту (T-148,
// зафиксированное решение): страница раз за разом не укладывается в
// render.timeout, рендерные попытки исчерпаны — итогом записи остаётся
// сырой HTML HTTP-фазы (rendered = false — признак «рендера не было»
// по схеме subdomain_results; отдельной колонки нет), запись уходит
// в q:analyze, конвейер доводит её анализом, работа не теряется.
// Доставка подтверждается (nil вызывающему processDelivery) — в q:dead
// запись не попадает. Механика и идемпотентность — finalizeNoRender.
func (w *Worker) finalizeSkipRender(ctx context.Context, task queue.Result, cause *tarpitError, attempts int) error {
	return w.finalizeNoRender(ctx, task,
		"рендер-тарпит: запись финализирована без рендера (итог — сырой HTML HTTP-фазы)",
		"attempts", attempts, "timeout_attempts", w.opt.TimeoutAttempts,
		"err", cause.Error())
}

// finalizeCertError — финализация записи без рендера по постоянной ошибке
// сертификата хоста (T-159, зафиксированное решение): рендер этого URL
// невозможен до смены сертификата самим хостом, ретраи бессмысленны и
// пережигали бы общий лимит доставок до q:dead (замер §12). Итог записи —
// сырой HTML HTTP-фазы (rendered = false — признак «рендера не было» по
// схеме subdomain_results; отдельной колонки для исхода рендера нет, а
// error принадлежит HTTP-проверке — ErrorClass T-058, перезапись лгала бы
// о ней; HTTP-результат домена уже сохранён, ТЗ §8 — некорректный
// сертификат загрузке не мешает, InsecureSkipVerify). Хост не заносится
// ни в какой список навсегда: следующий эпизод перепроверки (T-030)
// опубликует новую задачу q:render — с исправленным сертификатом рендер
// пройдёт. Доставка подтверждается (nil вызывающему processDelivery) —
// в q:dead запись не попадает. Механика и идемпотентность —
// finalizeNoRender.
func (w *Worker) finalizeCertError(ctx context.Context, task queue.Result, cause *certError) error {
	return w.finalizeNoRender(ctx, task,
		"рендер невозможен: постоянная ошибка сертификата хоста — запись финализирована без рендера (итог — сырой HTML HTTP-фазы)",
		"err", cause.Error())
}

// finalizeTaskError — финализация записи без рендера по ошибке класса
// задачи на последней попытке (T-172, зафиксированное решение): хост
// исчерпал доставки (redis.max_attempts) с ошибкой рендера класса task —
// отказ соединения, load error (isTaskRenderError), — ретраи уже даны,
// дальше путь только в q:dead; вместо захоронения итогом записи остаётся
// честный сырой HTML HTTP-фазы (rendered = false), запись уходит
// в q:analyze — конвейер доводит её анализом, работа не теряется
// (инцидент 23.10: смешанная серия таймаут→refused жгла все доставки
// и уносила 459 записей в q:dead за 11 минут, каждая смерть ≈ 3 лишних
// слот-времени рендера). Инфраструктурные ошибки (БД, хранилище,
// публикация, браузер) сюда не попадают — классом не task они умирают
// прежним путём. Хост не помечается навсегда: следующий эпизод
// перепроверки (T-030) опубликует новую задачу q:render — поднявшийся
// хост отрендерится. Доставка подтверждается (nil вызывающему
// processDelivery) — в q:dead запись не попадает. Механика и
// идемпотентность — finalizeNoRender.
func (w *Worker) finalizeTaskError(ctx context.Context, task queue.Result, cause error, attempts int) error {
	return w.finalizeNoRender(ctx, task,
		"рендер невозможен: ошибка класса задачи исчерпала доставки — запись финализирована без рендера (итог — сырой HTML HTTP-фазы)",
		"attempts", attempts, "max_attempts", w.opt.MaxAttempts,
		"err", cause.Error())
}

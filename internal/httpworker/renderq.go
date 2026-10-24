// Переполнение очереди рендера: флаг render_pending и доработка позже
// (T-066; ТЗ 5.6 «если очередь рендера переполнена, домены получают
// признак render_pending и дорабатываются позже; рендер не должен
// задерживать остальные этапы», §7 «домен считается done после завершения
// DNS/HTTP-этапа, а результаты рендера и анализа дополняют его
// асинхронно»).
//
// Штатный путь (§5.5 п. 8): SPA-записи результата (вердикт T-057 уже
// в Record.SPA) публикуются в q:render — по Result-сообщению на запись
// субдомен×вариант (формат T-024; потребитель T-067 заменит контент по
// ключу). Публикация — после фиксации транзакции результата, как у
// q:analyze (T-058): Result указывает на строку.
//
// Backpressure без блокировки. Гейт T-026 (WatermarkPublisher) ждёт
// ёмкости перед публикацией — для q:render это запрещено: OnResult
// выполняется до XACK задачи q:http (ТЗ 4.2), ожидание задержало бы
// подтверждение домена, т.е. рендер задерживал бы остальные этапы —
// против §5.6. Вместо ожидания — неблокирующая проверка глубины стрима
// (BackpressureGate.Len, T-026: backlog группы потребления) ДО
// транзакции результата:
//
//   - очередь свободна (depth < watermark, «строго ниже порога», ТЗ 4.2)
//     — q:render публикуется сразу после фиксации, мимо
//     WatermarkPublisher;
//   - очередь полна или глубина недоступна — публикация НЕ выполняется,
//     домен получает флаг render_pending (бит 1 domains.flags, T-008) ТОЙ
//     ЖЕ ТРАЗАКЦИЕЙ результата (§3.4: решение о рендере относится к этим
//     строкам и фиксируется атомарно с ними) и завершается как обычно —
//     check_state = done, XACK без задержки. Ошибка чтения глубины
//     трактуется как переполнение (консервативно): флаг сохраняет задачи
//     рендера от потери; настоящая недоступность Redis всё равно вернёт
//     доставку на публикации q:analyze (T-058).
//
// Порог — отдельный render.queue_high_watermark (T-174; расширение
// ТЗ 5.10): общий scheduler.queue_high_watermark (T-026) при клампе
// T-168 (min(lag, XLEN)+pending) держал ворота открытыми при лаге
// 98,4 тыс. из 100 тыс. — приток равнялся потреблению, дренаж долга
// нулевой (равновесие 24.10, этап 25 tasks.md). Дефолт 10 тыс. ≈ 2 ч
// буфера потребления (замер 24.10: ≈5,1 тыс. задач/ч) закрывает
// ворота на текущем долге и возвращает гистерезис «дренируем —
// впускаем»; 0 — общий порог. Эффективное значение (кламп
// min(watermark, MAXLEN/2), как у гейта T-026 — ворота обязаны
// закрываться до обрезки) считает проводка cmd/http-worker
// (renderQueueWatermark): сюда приходит готовое к сравнению число.
//
// Проход доработки (§5.6 «дорабатываются позже»): фоновый цикл
// RequeueRenderPending (стиль суточной перепроверки T-030 и очистки
// T-020) подбирает домены с флагом и дорабатывает их, когда очередь
// свободна. Выборка пачки — без PK-порядка, seq scan с ранним остановом
// LIMIT и закреплением плана SET LOCAL + guard счётчиком долга (T-181:
// index scan по PK вытаптывал дренированный «пустой» префикс — запрос №2
// журнала медленных 24.10, медиана 44 с; закреплённый seq scan — десятки
// мс, стационар без долга не трогает domains вовсе). SPA-вердикт берётся
// из сохранённого контента записи:
// DetectSPA (T-057) детерминирован над теми же байтами, что воркер
// проверял при записи, — вердикт совпадает; алиасы (content_hash NULL,
// alias_subdomain_id NOT NULL) наследуют вердикт субдомена-оригинала —
// класс эквивалентности T-055 един по определению (один итоговый URL
// или один контент), вердикты внутри класса совпадают. Порядок внутри
// домена — «публикация q:render → снятие флага» (переходы бита ведут
// счётчик рендер-долга одной транзакцией с битом, T-175; §3.4, в духе
// T-039:
// сбой после публикации оставит флаг — повтор прохода опубликует снова,
// дубликаты ограничены и гасятся идемпотентностью потребителя по ключу;
// обратный порядок терял бы задачи навсегда — снятый флаг вернуть
// некому).
//
// Ограничения прохода (фиксируются сознательно):
//   - записи без собственного контента и без алиаса (контент не сохранён
//     — лимит/переполнение диска, T-056) не переиздаются: вердикт взять
//     неоткуда; их доработает регулярная перепроверка (T-030);
//   - сбой чтения блока (транзиентный сбой хранилища, гонка с
//     компактификацией) флаг домена НЕ снимает: проход повторит чтение
//     на следующем тике — цена минуты повтора против потери SPA-задачи
//     до перепроверки T-030 (месяцы);
//   - наследование алиаса — по субдомену-оригиналу без варианта
//     (alias_subdomain_id ссылается на субдомен, T-055): у оригинала с
//     двумя вариантами разных сайтов наследуются оба — редкий случай,
//     цена — лишний рендер статической страницы, не потеря данных;
//   - флаг не стирается записью нового эпизода: пока проход не снял,
//     новый эпизод с SPA и свободной очередью публикует задачи сам
//     (дубликаты ограничены), без SPA — проход снимет флаг, не найдя
//     SPA-записей. Гонка «проход снимает флаг, а повторная доставка
//     эпизода ставит его снова» сужена охраной бита в UPDATE снятия
//     (RowsAffected = 0 — конкурент уже решил); окно требует доставки,
//     не дошедшей до XACK, — попытки доставки исчерпываются за минуты
//     (T-025), а самозалечивает остаток перепроверка T-030.
package httpworker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"

	"crawler/internal/content"
	"crawler/internal/db"
	"crawler/internal/httpfetch"
	"crawler/internal/model"
	"crawler/internal/queue"
)

// Дефолты прохода доработки.
const (
	// DefaultRenderRequeueInterval — период прохода доработки рендера.
	// Минута, как DefaultCleanupInterval (T-020): переполнение — аварийный
	// режим, «дорабатываются позже» не означает часы; при полной очереди
	// проход стоит одно чтение глубины и не трогает БД. Отдельной опции
	// конфига нет (§5.10 состав фиксирован) — интервал, как расписание
	// T-030, живёт опцией цикла.
	DefaultRenderRequeueInterval = time.Minute

	// DefaultRenderRequeueBatch — доменов в одной пачке прохода: проход
	// читает контент записей для повторного SPA-вердикта (тела до 2 МиБ,
	// ТЗ 5.5) — пачка умеренная против recheck-пачек T-030 (тем читать
	// нечего); ёмкость q:render перепроверяется между доменами.
	DefaultRenderRequeueBatch = 100
)

// SQL прохода и флага. Коды схемы — литералами (стиль T-039); бит
// render_pending — 2, маска снятия — 0xFFFD (все биты, кроме него).
// T-175: переходы бита ведут инкрементальный счётчик рендер-долга
// crawler_counters['render_pending'] — ±1 в той же транзакции, что и
// переход (наблюдаемость долга без seq scan по 41,8 млн строк;
// частичный индекс по flags отвергнут — ломает HOT, docs/schema.md
// «HOT-апдейты», обоснование — миграция 000015).
var (
	// renderSetPendingSQL — флаг render_pending в транзакции записи
	// результата (T-066): бит 1 через OR — is_idn (бит 0) и прочие биты
	// сохраняются; колонка domains.flags — smallint, побитовые операторы
	// PostgreSQL повышают smallint до int — возврат через ::int2.
	// Охрана бита (T-175): семантика OR не меняется (повторная установка
	// бита — тот же результат), но RowsAffected становится индикатором
	// РЕАЛЬНОГО перехода 0→1 — по нему счётчик долга наращивается ровно
	// на переходы, а не на каждую повторную доставку с уже стоящим
	// флагом (зеркально охране снятия ниже).
	renderSetPendingSQL = "UPDATE domains AS t " +
		"SET flags = (t.flags | " + fmt.Sprintf("%d", int16(model.FlagRenderPending)) + ")::int2 " +
		"WHERE t.zone_id = $1::int2 AND t.id = $2::int8 " +
		"AND (t.flags & " + fmt.Sprintf("%d", int16(model.FlagRenderPending)) + ") = 0"

	// renderPendingDomainsSQL — пачка доменов, ждущих доработки рендера:
	// бит render_pending выставлен, домен не удалён (§13.1 — вне цикла
	// проверок). Имя — label || '.' || зона (стиль T-027) для Result.Name
	// и логов; LIMIT — пачка. ORDER BY по PK отсутствует сознательно
	// (T-181, решение оператора 24.10 — порядок доменов не важен): сортировка
	// держала index scan по PK, который при живом долге вытаптывает
	// дренированный «пустой» префикс PK-порядка (запрос №2 журнала
	// медленных 24.10: 14,1 млн Rows Removed, ≈4,5 ГБ буферов за вызов,
	// медиана 44 с); недетерминированные пачки корректны — снятие флага
	// после публикации исключает повторы. Форма плана (seq scan с ранним
	// остановом LIMIT) закреплена SET LOCAL в транзакции выборки —
	// renderPlanNoIndexScanSQL/renderPlanNoBitmapScanSQL ниже.
	renderPendingDomainsSQL = "SELECT d.zone_id, d.id, d.label || '.' || z.name " +
		"FROM domains AS d JOIN zones AS z ON z.id = d.zone_id " +
		"WHERE (d.flags & " + fmt.Sprintf("%d", int16(model.FlagRenderPending)) + ") <> 0 " +
		"AND d.status <> " + fmt.Sprintf("%d", int16(model.StatusDeleted)) + " " +
		"LIMIT $1"

	// renderPlanNoIndexScanSQL/renderPlanNoBitmapScanSQL/renderPlanNoIOSQL —
	// закрепление формы плана выборки пачки (T-181): SET LOCAL в транзакции
	// renderSelectPending отключает index scan, bitmap scan и index-only
	// scan до оператора выборки. Статистики выражения (flags & 2) <> 0 у
	// планировщика нет (индекс по flags невозможен — HOT-инвариант, T-175),
	// фильтр оценивается почти неселективным (41,6 из 41,6 млн строк), и без
	// закрепления планировщик берёт index scan по PK даже без ORDER BY —
	// то же вытаптывание префикса; закреплённый seq scan находит первую
	// пачку флаговых строк ранним остановом LIMIT (флаговый долг .рф
	// физически в начале кучи: ingest ru/rf/su шёл первым, HOT держит
	// версии на родных страницах) — 43 мс против 30 с на замере 24.10
	// (700×). Действие SET LOCAL — только эта транзакция, остальные
	// запросы пула не затронуты; bitmap- и index-only-скан отключаются
	// вслед за index-сканом — все индексные формы доступа к domains
	// закрыты (index-only по PK повторил бы патологию, хоть ТЗ §4.2 и
	// проверяло диагностикой только пару index/bitmap), по flags индексов
	// не будет.
	renderPlanNoIndexScanSQL  = "SET LOCAL enable_indexscan = off"
	renderPlanNoBitmapScanSQL = "SET LOCAL enable_bitmapscan = off"
	renderPlanNoIOSQL         = "SET LOCAL enable_indexonlyscan = off"

	// renderDebtCounterSQL — guard прохода точечным чтением счётчика
	// рендер-долга (T-181; счётчик — T-175): value = 0 — тяжёлая выборка
	// пачки не выполняется вовсе (стационар без долга стоит одно чтение
	// строки за тик вместо seq scan по 41,8 млн строк); отсутствие строки —
	// ошибка прохода (строку гарантирует миграция 000015; молчаливый
	// рассинхрон сделал бы guard ложью — философия renderBumpDebtCounter,
	// runbooks §6).
	renderDebtCounterSQL = "SELECT value FROM crawler_counters WHERE name = 'render_pending'"

	// renderPendingRowsSQL — записи домена для повторного SPA-вердикта:
	// ещё не отрендеренные (rendered = false ставит запись результата
	// T-058, true — рендер T-067) и с итоговым URL (цель рендера).
	// content_hash — тело для DetectSPA (T-057/T-056), alias_subdomain_id
	// — наследование вердикта оригинала (T-055). ORDER BY —
	// детерминированный порядок публикации.
	renderPendingRowsSQL = "SELECT subdomain_id, variant, content_hash, alias_subdomain_id " +
		"FROM subdomain_results " +
		"WHERE zone_id = $1::int2 AND domain_id = $2::int8 " +
		"AND rendered = false AND final_url IS NOT NULL " +
		"ORDER BY subdomain_id, variant"

	// renderClearPendingSQL — снятие флага после публикации. Охрана бита:
	// конкурентное снятие (второй экземпляр прохода, соседний инстанс
	// сервиса) даёт RowsAffected = 0 без ошибки — дубликаты публикации
	// ограничены одним окном прохода. Маска — знаковое ~бита (−3): биты
	// smallint повышаются до int4 со знаковым расширением, беззнаковая
	// маска 65533 не влезает в smallint по значению (проверено на живом
	// PostgreSQL 16), знаковая очищает ровно бит render_pending и
	// сохраняет остальные, включая старший.
	renderClearPendingSQL = "UPDATE domains AS t " +
		"SET flags = (t.flags & " + fmt.Sprintf("%d", int16(^int16(model.FlagRenderPending))) + ")::int2 " +
		"WHERE t.zone_id = $1::int2 AND t.id = $2::int8 " +
		"AND (t.flags & " + fmt.Sprintf("%d", int16(model.FlagRenderPending)) + ") <> 0"

	// renderCounterDeltaSQL — сдвиг счётчика рендер-долга (T-175):
	// crawler_counters['render_pending'] ±1 В ТОЙ ЖЕ ТРАНЗАКЦИИ, что и
	// переход бита (§3.4: бит и счётчик не расходятся — откат откатывает
	// оба). Дельта — параметр (int8: +1 постановка флага в записи
	// результата, −1 снятие проходом доработки), имя строки — литерал
	// (стиль T-039). RowsAffected 0 — строки нет (возможна только ручная
	// правка таблицы, runbooks §6): ошибка вызывающего откатывает
	// транзакцию целиком — молчаливый рассинхрон счётчика недопустим,
	// долг обязан быть виден честно (постановка T-175).
	renderCounterDeltaSQL = "UPDATE crawler_counters AS c " +
		"SET value = c.value + $1::int8 " +
		"WHERE c.name = 'render_pending'"
)

// RenderLen — источник глубины стрима: удовлетворяется
// *queue.BackpressureGate (T-026). Узкий интерфейс — httpworker не
// зависит от Redis-специфики гейта, юнит-тесты подменяют фейком.
type RenderLen interface {
	Len(ctx context.Context, stream string) (int64, error)
}

// RenderContent — чтение сохранённого контента для повторного
// SPA-вердикта в проходе доработки; удовлетворяется *content.PackStore
// (T-042).
type RenderContent interface {
	Get(ctx context.Context, hash content.Hash) ([]byte, error)
}

// RenderQueue — публикация q:render без ожидания ёмкости (§5.6: рендер
// не задерживает остальные этапы). Очередь q — публикация мимо
// WatermarkPublisher (T-026): его Publish ждал бы ёмкости и задержал XACK
// задачи q:http; ёмкость проверяется отдельно (HasCapacity) — решением
// «публиковать или флажить» распоряжается вызывающий. Потокобезопасен:
// состояние неизменно после New.
type RenderQueue struct {
	q         queue.Queue
	lenner    RenderLen
	watermark int64
	log       *slog.Logger
}

// NewRenderQueue строит очередь рендера поверх «сырой» очереди (без
// гейта backpressure) и источника глубины стримов; watermark — порог
// ёмкости q:render render.queue_high_watermark (T-174; 0 — общий
// scheduler.queue_high_watermark; эффективное значение с клампом
// MAXLEN/2 считает проводка cmd/http-worker — сюда приходит готовое
// к сравнению число). nil-зависимости — ошибка: тихая потеря
// публикаций незаметна для оператора.
func NewRenderQueue(q queue.Queue, lenner RenderLen, watermark int64, log *slog.Logger) (*RenderQueue, error) {
	if q == nil {
		return nil, errors.New("httpworker: очередь рендера: очередь не задана")
	}
	if lenner == nil {
		return nil, errors.New("httpworker: очередь рендера: источник длины стрима не задан (BackpressureGate, T-026)")
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &RenderQueue{q: q, lenner: lenner, watermark: watermark, log: log}, nil
}

// HasCapacity — неблокирующая проверка ёмкости q:render: true, когда
// глубина стрима строго ниже порога render.queue_high_watermark
// (T-174; depth == watermark — уже переполнение, ТЗ 4.2). Watermark ≤ 0
// отключает проверку (семантика T-026) — ёмкость есть всегда, без
// обращения к Redis.
func (rq *RenderQueue) HasCapacity(ctx context.Context) (bool, error) {
	if rq.watermark <= 0 {
		return true, nil
	}
	n, err := rq.lenner.Len(ctx, queue.StreamRender)
	if err != nil {
		return false, err
	}
	return n < rq.watermark, nil
}

// Publish публикует Result-сообщения в q:render без проверки ёмкости:
// проверка всегда отдельным шагом (HasCapacity) — воркер записи решает
// «публиковать или флажить» до транзакции, проход — «пора ли домен».
func (rq *RenderQueue) Publish(ctx context.Context, msgs []queue.Result) error {
	for i := range msgs {
		if _, err := rq.q.Publish(ctx, queue.StreamRender, msgs[i]); err != nil {
			return fmt.Errorf("httpworker: публикация q:render (запись %d/%d): %w", i+1, len(msgs), err)
		}
	}
	return nil
}

// renderSPAResults — Result-сообщения q:render по SPA-записям результата
// (§5.5 п. 8): по одному ключу субдомен×вариант на запись с вердиктом
// SPA=true (T-057). Алиасные записи несут вердикт своей проверки-
// источника — класс эквивалентности T-055 един, вердикты внутри класса
// совпадают; потребитель T-067 видит alias_kind строки и сам решает,
// что делать с алиасом. nil-вердикт (страницы нет) рендера не требует.
func renderSPAResults(res *DomainResult) []queue.Result {
	var out []queue.Result
	for i := range res.Subdomains {
		sub := &res.Subdomains[i]
		for j := range sub.Records {
			rec := &sub.Records[j]
			if rec.SPA == nil || !rec.SPA.SPA {
				continue
			}
			out = append(out, queue.Result{
				ZoneID:      res.ZoneID,
				DomainID:    res.DomainID,
				SubdomainID: sub.SubdomainID,
				Variant:     rec.Variant,
				Name:        res.Name,
			})
		}
	}
	return out
}

// NewRenderRecorder — записатель результата с рендер-конвейером (T-066):
// NewRecorder (T-058) + очередь рендера. Отдельный конструктор, а не
// новый параметр NewRecorder: сигнатура T-058 закреплена его тестами;
// render = nil у NewRecorder сохраняет прежнее поведение (без q:render и
// флага) для изолированных сборок.
func NewRenderRecorder(pool RecordDB, q queue.Queue, render *RenderQueue, log *slog.Logger) (*Recorder, error) {
	r, err := NewRecorder(pool, q, log)
	if err != nil {
		return nil, err
	}
	r.render = render
	return r, nil
}

// --- проход доработки ---------------------------------------------------------

// RequeueOptions — параметры прохода доработки рендера; нулевые значения
// заменяются дефолтами (withDefaults, стиль RecheckOptions T-030).
type RequeueOptions struct {
	// Interval — период цикла RequeueRenderPending
	// (DefaultRenderRequeueInterval); на разовый RequeueOnce не влияет.
	Interval time.Duration
	// BatchSize — максимум доменов в одной пачке
	// (DefaultRenderRequeueBatch).
	BatchSize int
	// Log — логгер цикла и Warn-и прохода; nil — записи подавлены
	// (slog.DiscardHandler).
	Log *slog.Logger
}

func (o RequeueOptions) withDefaults() RequeueOptions {
	if o.Interval <= 0 {
		o.Interval = DefaultRenderRequeueInterval
	}
	if o.BatchSize <= 0 {
		o.BatchSize = DefaultRenderRequeueBatch
	}
	return o
}

// RequeueResult — итог одного прохода RequeueOnce.
type RequeueResult struct {
	// Selected — доменов выбрано с флагом (размер пачки выбора).
	Selected int
	// Cleared — доменов снят флаг (после публикации или за ненадобностью
	// — SPA-записей не нашлось).
	Cleared int
	// Published — опубликовано сообщений q:render.
	Published int
	// Full — проход остановлен: q:render у порога; необработанные домены
	// сохранили флаг, доработка продолжится следующим проходом.
	Full bool
}

// renderPendingDomain — домен пачки прохода.
type renderPendingDomain struct {
	zone int16
	id   int64
	name string
}

// renderPendingRow — запись субдомен×вариант домена прохода.
type renderPendingRow struct {
	subdomain int16
	variant   model.Variant
	hash      []byte // nil — собственного контента нет (алиас либо не сохранён, T-056)
	aliasSub  *int16 // не nil — алиас: вердикт наследуется от оригинала (T-055)
}

// RequeueOnce — один проход доработки рендера (§5.6 «дорабатываются
// позже»): до opts.BatchSize доменов с флагом render_pending. Ёмкость
// q:render проверяется до выборки и перед публикацией каждого домена —
// при переполнении проход останавливается (Full), флаги сохранены; при
// нулевом счётчике долга (guard T-181) тяжёлая выборка не выполняется.
// Для домена — ключи SPA-записей (повторный вердикт по сохранённому
// контенту) → публикация q:render → снятие флага со счётчиком долга
// одной транзакцией (T-175). Пустой набор ключей — флаг снимается без
// публикации (черновой флаг: страница перестала быть SPA); нечитаемый
// контент флаг сохраняет до следующего прохода (renderDomainKeys).
// Пул с транзакциями (RecordDB): снятие бита и декремент счётчика
// рендер-долга обязаны фиксироваться вместе (§3.4, T-175).
func RequeueOnce(ctx context.Context, pool RecordDB, contents RenderContent, render *RenderQueue, opts RequeueOptions) (RequeueResult, error) {
	if pool == nil {
		return RequeueResult{}, errors.New("httpworker: проход рендера: пул не задан")
	}
	if contents == nil {
		return RequeueResult{}, errors.New("httpworker: проход рендера: хранилище контента не задано")
	}
	if render == nil {
		return RequeueResult{}, errors.New("httpworker: проход рендера: очередь рендера не задана")
	}
	o := opts.withDefaults()
	if o.Log == nil {
		o.Log = slog.New(slog.DiscardHandler)
	}
	out := RequeueResult{}

	// Полная очередь — БД не читается (одно чтение глубины):
	// переполнение и есть причина флагов; сигнал «стало свободно» —
	// следующий тик цикла.
	free, err := render.HasCapacity(ctx)
	if err != nil {
		return out, fmt.Errorf("httpworker: проход рендера: ёмкость q:render: %w", err)
	}
	if !free {
		return RequeueResult{Full: true}, nil
	}

	// Guard счётчиком рендер-долга (T-181, счётчик — T-175): стационар без
	// долга не платит за выборку пачки — точечное чтение строки счётчика
	// вместо seq scan по всей domains раз в тик; 0 → пустая пачка (Selected
	// = 0 — drain корректно останавливается). Отрицательное значение кодом
	// не порождается (переходы бита ведут счётчик парой ±1, T-175) и
	// трактуется так же — как «долга нет», но громко: тихий баг счётчика
	// не должен прятаться за молчаливым простоем дренажа.
	debt, err := renderDebtCount(ctx, pool)
	if err != nil {
		return out, err
	}
	if debt < 0 {
		o.Log.Warn("проход рендера: отрицательный счётчик рендер-долга — рассинхрон (runbooks §6)", "debt", debt)
	}
	if debt <= 0 {
		return out, nil
	}

	domains, err := renderSelectPending(ctx, pool, o.BatchSize)
	if err != nil {
		return out, err
	}
	out.Selected = len(domains)
	if len(domains) == 0 {
		// Счётчик видит долг, выборка — нет: биты-«сироты» (удалённые с
		// флагом домены — выборка честно исключает их по §13.1, а снять
		// бит некому) либо рассинхрон ручного SQL (runbooks §6). Дренаж
		// платит полный проход без раннего останова за каждый тик —
		// оператор обязан это видеть.
		o.Log.Warn("проход рендера: счётчик долга > 0, выборка пуста — биты-«сироты» либо рассинхрон счётчика (runbooks §6)",
			"debt", debt)
		return out, nil
	}

	for i := range domains {
		if err := ctx.Err(); err != nil {
			return out, fmt.Errorf("httpworker: проход рендера: %w", err)
		}
		d := &domains[i]
		keys, keep, err := renderDomainKeys(ctx, pool, contents, d, o.Log)
		if err != nil {
			return out, err
		}
		if len(keys) > 0 {
			free, err := render.HasCapacity(ctx)
			if err != nil {
				return out, fmt.Errorf("httpworker: проход рендера: ёмкость q:render: %w", err)
			}
			if !free {
				out.Full = true
				return out, nil
			}
			if err := render.Publish(ctx, keys); err != nil {
				return out, fmt.Errorf("httpworker: проход рендера: домен %s: %w", d.name, err)
			}
			out.Published += len(keys)
		}
		if keep {
			// Контент части записей не прочитан (транзиентный сбой):
			// флаг сохраняется — следующий проход повторит домен;
			// снятие сняло бы SPA-задачу до перепроверки T-030.
			continue
		}
		// Снятие флага — после публикации (§3.4, порядок в шапке файла),
		// со счётчиком долга одной транзакцией (T-175).
		cleared, err := renderClearPendingTx(ctx, pool, d.zone, d.id)
		if err != nil {
			return out, fmt.Errorf("httpworker: проход рендера: домен %s: снятие флага: %w", d.name, err)
		}
		if cleared > 0 {
			out.Cleared++
		}
	}
	return out, nil
}

// renderClearPendingTx — снятие флага render_pending и декремент счётчика
// рендер-долга одной транзакцией (T-175): возвращает RowsAffected снятия
// (> 0 — бит был и снят, счётчик −1; 0 — бита уже не было (охрана бита:
// конкурентное снятие вторым экземпляром прохода), счётчик не тронут).
// Сбой любого шага откатывает пару целиком — бит и счётчик не
// расходятся; повторная попытка (следующий тик прохода / повторная
// доставка) выполнит пару заново.
func renderClearPendingTx(ctx context.Context, pool RecordDB, zone int16, id int64) (int64, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("httpworker: транзакция снятия: %w", err)
	}
	// Откат безусловно: после Commit — no-op (стиль T-044).
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx, renderClearPendingSQL, zone, id)
	if err != nil {
		return 0, err
	}
	if tag.RowsAffected() > 0 {
		if err := renderBumpDebtCounter(ctx, tx, -1); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("httpworker: фиксация снятия: %w", err)
	}
	return tag.RowsAffected(), nil
}

// renderBumpDebtCounter — сдвиг счётчика рендер-долга в ОТКРЫТОЙ
// транзакции перехода бита render_pending (T-175): delta ±1. Возврат с
// ошибкой откатывает транзакцию вызывающего — бит и счётчик меняются
// только вместе (§3.4). RowsAffected != 1 — строки 'render_pending' нет
// (создаёт и бэкфиллит миграция 000015; пропасть может только при ручной
// правке таблицы, runbooks §6): молчаливый рассинхрон сделал бы gauge
// долга ложью — ошибка громче.
func renderBumpDebtCounter(ctx context.Context, tx pgx.Tx, delta int64) error {
	tag, err := tx.Exec(ctx, renderCounterDeltaSQL, delta)
	if err != nil {
		return fmt.Errorf("счётчик рендер-долга: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("счётчик рендер-долга: затронуто %d строк (строка 'render_pending' обязательна, миграция 000015)",
			tag.RowsAffected())
	}
	return nil
}

// renderDebtCount — guard прохода доработки (T-181): точечное чтение
// счётчика рендер-долга crawler_counters['render_pending'] (T-175).
// 0 — долга нет, тяжёлая выборка пачки (seq scan по domains) не
// выполняется вовсе; отсутствие строки — ошибка: строку создаёт бэкфилл
// миграции 000015, пропасть она может только при ручной правке таблицы
// (runbooks §6) — молчаливый рассинхрон сделал бы guard ложью, громче
// ошибкой (как у renderBumpDebtCounter).
func renderDebtCount(ctx context.Context, pool RecordDB) (int64, error) {
	rows, err := pool.Query(ctx, renderDebtCounterSQL)
	if err != nil {
		return 0, fmt.Errorf("httpworker: проход рендера: счётчик долга: %w", err)
	}
	defer rows.Close()
	var n int64
	if !rows.Next() {
		// Сначала сбой чтения: обрыв соединения — не «строки нет»,
		// диагноз отсутствия строки должен быть честным (runbooks §6
		// ведёт оператора делать INSERT).
		if err := rows.Err(); err != nil {
			return 0, fmt.Errorf("httpworker: проход рендера: счётчик долга: %w", err)
		}
		return 0, errors.New("httpworker: проход рендера: строка счётчика 'render_pending' отсутствует (миграция 000015, runbooks §6)")
	}
	if err := rows.Scan(&n); err != nil {
		return 0, fmt.Errorf("httpworker: проход рендера: счётчик долга: чтение: %w", err)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("httpworker: проход рендера: счётчик долга: %w", err)
	}
	return n, nil
}

// renderSelectPending выбирает пачку доменов с флагом render_pending —
// единая точка тяжёлой выборки прохода. Выборка идёт в явной транзакции с
// SET LOCAL enable_indexscan/bitmapscan = off (T-181): закрепление формы
// плана (seq scan по domains + ранний останов LIMIT) — см. комментарий к
// renderPlanNoIndexScanSQL; без закрепления планировщик без статистики
// выражения (flags & 2) <> 0 берёт index scan по PK, вытаптывающий
// дренированный «пустой» префикс (медиана 44 с на живом долге, замер
// 24.10). Транзакция — только оболочка закрепления: SELECT читает
// зафиксированные данные, фиксация/откат по содержанию равнозначны.
func renderSelectPending(ctx context.Context, pool RecordDB, limit int) ([]renderPendingDomain, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("httpworker: проход рендера: транзакция выборки: %w", err)
	}
	// Откат безусловно: после Commit — no-op (стиль T-044).
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, renderPlanNoIndexScanSQL); err != nil {
		return nil, fmt.Errorf("httpworker: проход рендера: закрепление плана: %w", err)
	}
	if _, err := tx.Exec(ctx, renderPlanNoBitmapScanSQL); err != nil {
		return nil, fmt.Errorf("httpworker: проход рендера: закрепление плана: %w", err)
	}
	if _, err := tx.Exec(ctx, renderPlanNoIOSQL); err != nil {
		return nil, fmt.Errorf("httpworker: проход рендера: закрепление плана: %w", err)
	}

	rows, err := tx.Query(ctx, renderPendingDomainsSQL, limit)
	if err != nil {
		return nil, fmt.Errorf("httpworker: проход рендера: выборка доменов: %w", err)
	}
	defer rows.Close()

	var out []renderPendingDomain
	for rows.Next() {
		var d renderPendingDomain
		if err := rows.Scan(&d.zone, &d.id, &d.name); err != nil {
			return nil, fmt.Errorf("httpworker: проход рендера: выборка доменов: чтение: %w", err)
		}
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("httpworker: проход рендера: выборка доменов: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("httpworker: проход рендера: фиксация выборки: %w", err)
	}
	return out, nil
}

// renderDomainKeys — ключи q:render домена прохода: повторный SPA-вердикт
// по сохранённому контенту. Записи с content_hash читаются из хранилища
// (DetectSPA детерминирован — тот же вердикт, что при записи, T-057);
// алиасы наследуют вердикт субдомена-оригинала; записи без контента и
// без алиаса пропускаются (контент не сохранён, T-056 — доработает
// перепроверка T-030). Ошибка чтения блока — не ошибка прохода, но и не
// повод снимать флаг: third возврат keep=true оставляет render_pending
// (транзиентный сбой хранилища или гонка компактификации не должны
// терять SPA-задачу до перепроверки T-030) — запись пропускается с Warn,
// проход продолжается, следующий тик повторит домен; отмена ctx —
// ошибка прохода.
func renderDomainKeys(ctx context.Context, pool db.DB, contents RenderContent, d *renderPendingDomain, log *slog.Logger) (keys []queue.Result, keep bool, err error) {
	rows, err := pool.Query(ctx, renderPendingRowsSQL, d.zone, d.id)
	if err != nil {
		return nil, false, fmt.Errorf("httpworker: проход рендера: домен %s: записи: %w", d.name, err)
	}
	defer rows.Close()

	var recs []renderPendingRow
	for rows.Next() {
		var (
			r        renderPendingRow
			variant  int16
			hash     []byte
			aliasSub *int16
		)
		if err := rows.Scan(&r.subdomain, &variant, &hash, &aliasSub); err != nil {
			return nil, false, fmt.Errorf("httpworker: проход рендера: домен %s: записи: чтение: %w", d.name, err)
		}
		r.variant, r.hash, r.aliasSub = model.Variant(variant), hash, aliasSub
		recs = append(recs, r)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("httpworker: проход рендера: домен %s: записи: %w", d.name, err)
	}

	// Свои вердикты — по контенту; порядок ключей детерминирован
	// (ORDER BY запроса), алиасы — после своих.
	spa := make(map[int16]bool)
	keys = make([]queue.Result, 0, len(recs))
	var aliases []renderPendingRow
	for _, r := range recs {
		if r.aliasSub != nil {
			aliases = append(aliases, r)
			continue
		}
		if len(r.hash) != content.HashLen {
			// Контента нет (T-056) либо хэш недопустимой длины —
			// страховка прямых вызовов; вердикт взять неоткуда.
			continue
		}
		var h content.Hash
		copy(h[:], r.hash)
		body, err := contents.Get(ctx, h)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil, false, fmt.Errorf("httpworker: проход рендера: домен %s: чтение контента: %w", d.name, err)
			}
			// Транзиентный сбой: флаг домена сохраняется (keep),
			// следующий проход повторит чтение.
			log.Warn("проход рендера: контент записи недоступен — домен останется с флагом",
				"domain", d.name, "subdomain_id", r.subdomain, "err", err)
			keep = true
			continue
		}
		if httpfetch.DetectSPA(body).SPA {
			spa[r.subdomain] = true
			keys = append(keys, queue.Result{
				ZoneID: d.zone, DomainID: d.id,
				SubdomainID: r.subdomain, Variant: r.variant, Name: d.name,
			})
		}
	}
	for _, a := range aliases {
		if spa[*a.aliasSub] {
			keys = append(keys, queue.Result{
				ZoneID: d.zone, DomainID: d.id,
				SubdomainID: a.subdomain, Variant: a.variant, Name: d.name,
			})
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].SubdomainID != keys[j].SubdomainID {
			return keys[i].SubdomainID < keys[j].SubdomainID
		}
		return keys[i].Variant < keys[j].Variant
	})
	return keys, keep, nil
}

// RequeueRenderPending — цикл проходов доработки рендера (§5.6
// «дорабатываются позже»): стартовый дренаж пачками до исчерпания либо
// до полного q:render (подбор флагов, накопленных за простоем, — стиль
// CleanupDeleted T-020 / RecheckDaily T-030), далее тикер Interval.
// Ошибка прохода — Warn, цикл живёт (контракт OnRun, T-012): очередной
// проход продолжит с того же места — снятое не снимается дважды,
// неопубликованное сохраняет флаг. Отмена ctx — чистый nil (graceful
// shutdown, §3.4).
func RequeueRenderPending(ctx context.Context, pool RecordDB, contents RenderContent, render *RenderQueue, opts RequeueOptions) error {
	if pool == nil {
		return errors.New("httpworker: проход рендера: пул не задан")
	}
	if contents == nil {
		return errors.New("httpworker: проход рендера: хранилище контента не задано")
	}
	if render == nil {
		return errors.New("httpworker: проход рендера: очередь рендера не задана")
	}
	o := opts.withDefaults()
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log.Info("проход доработки рендера запущен",
		"interval", o.Interval.String(), "batch", o.BatchSize)

	drain := func() {
		for {
			res, err := RequeueOnce(ctx, pool, contents, render, o)
			if err != nil {
				// Отмена во время прохода — часть остановки, не тревога.
				if ctx.Err() == nil {
					log.Warn("проход доработки рендера", "err", err)
				}
				return
			}
			if res.Published > 0 || res.Cleared > 0 {
				log.Info("проход доработки рендера",
					"domains", res.Cleared, "published", res.Published, "full", res.Full)
			}
			// Полная пачка — возможно, остались домены; полная очередь —
			// ждать тика (публикация «впритык к порогу» не нужна).
			// Пачка БЕЗ снятых флагов — тоже ждать тика: контент не
			// читается (keep), стартовый дренаж не крутится вхолостую —
			// T-181 убрал естественный темпитель дорогой выборкой (замер
			// 24.10: 137 срабатываний журнала за 2,7 ч ≈ темп drain),
			// голый цикл по битым блокам молотил бы БД без останова.
			if res.Full || res.Selected < o.BatchSize || res.Cleared == 0 {
				return
			}
		}
	}

	drain()
	ticker := time.NewTicker(o.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			drain()
		}
	}
}

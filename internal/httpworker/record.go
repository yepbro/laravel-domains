// Запись результата HTTP-эпизода в subdomain_results и публикация q:analyze
// (T-058; ТЗ 5.5 пп. 7, 9, §6.1, §7, §8, §3.4). Файл замыкает конвейер
// этапа: DomainResult (T-053) после сохранения контента (ContentSaver,
// T-056) превращается в строки subdomain_results одним пакетом, домен
// получает итоговый статус и done, каждая запись публикуется в q:analyze
// (Result, T-024). Точка вызова — Options.OnResult в сборке сервиса
// (cmd/http-worker), вслед за ContentSaver и до XACK.
//
// Статус домена (§5.5 п. 9, §7): есть записи (хоть один субдомен ответил
// по хоть одному протоколу) → alive; записей нет и среди проверок есть
// таймауты (ErrTimeout) → temp_error — «SERVFAIL, таймауты» (ТЗ 6.2),
// повтор эпизода назначит планировщик (T-031); записей нет и веб молчит
// постоянно → no_http. check_state = done — домен завершён после DNS/HTTP
// (§7: «домен считается done после завершения DNS/HTTP-этапа, а
// результаты рендера и анализа дополняют его асинхронно»); rendered =
// false (DOM заменит сырой HTML рендер, T-067), rules_version = NULL
// (проставит анализатор, T-068+). queued_at = NULL и last_checked_at =
// now() — стиль T-039: lease завершён вместе с эпизодом (инвариант
// T-018/T-029/T-030), время — сервер БД; temp_attempts сбрасывается на
// каждом не-temp_error итоге (контракт T-031, как у DNS-этапа T-039).
//
// Порядок операций (ТЗ 3.4, §5.5 п. 9 «после записи результата
// публикуется задача в q:analyze»; ТЗ 4.2 «XACK только после записи
// результата в БД»):
//
//		запись в БД (одна транзакция) → публикация q:analyze → XACK
//
//	  - запись и своп ссылок на контент — ОДНА транзакция (SwapRefTx, T-044:
//	    раздельные транзакции опасны в обе стороны — строка без ссылки или
//	    ссылка без строки). Внутри: UPDATE domains (охрана удаления) →
//	    DELETE старых строк субдомен×вариант с RETURNING content_hash →
//	    −1 refcount каждого старого хэша → INSERT новых строк;
//	  - публикация q:analyze — ПОСЛЕ фиксации транзакции: потребитель Result
//	    (анализатор, T-068+) читает запись из БД по ключу — сообщение раньше
//	    строки смотрело бы в пустоту. Сбой публикации → доставка q:http без
//	    XACK: повторная доставка перезапишет тот же результат (идемпотентно,
//	    см. ниже) и опубликует q:analyze заново — дубликаты сообщений
//	    ограничены попытками доставки (at-least-once, ТЗ 3.4; потребитель
//	    обязан быть идемпотентным по ключу — тот же контракт, что у XACK,
//	    потерявшего ответ);
//	  - публикация q:render (SPA-вердикты T-057) — T-066, renderq.go:
//	    неблокирующая проверка ёмкости q:render ДО транзакции; переполнение
//	    — флаг render_pending ТОЙ ЖЕ транзакцией (§3.4), при свободной
//	    очереди — публикация после фиксации мимо WatermarkPublisher
//	    (§5.6: рендер не задерживает остальные этапы — XACK не ждёт
//	    ёмкости).
//
// Идемпотентность повторной доставки (ТЗ 3.4): перезапись результата —
// DELETE+INSERT строк домена (§3.5 «история проверок не хранится: только
// последний результат» — удаляются ВСЕ строки домена, включая ключи,
// которых нет в новом результате, например пропавший субдомен). Охрана
// одна — status <> deleted (§8/§13.1: удалённому во время проверки домену
// результат не записывается; строка блокируется UPDATE-ом и delete-файл
// T-019 либо успевает раньше — запись пропускается, либо позже — статус
// перетирается в deleted, оставшиеся строки вычистит T-020). Охраны
// check_state <> done (как у T-039) НЕТ, и это осознанный выбор: порядок
// «запись → публикация» оставляет окно «записано, q:analyze не
// опубликовано» — повтор доставки обязан-publishнуть заново, а фильтр
// done отбросил бы публикацию навсегда (анализ ждал бы перепроверки
// T-030, ~полгода). Плата — теоретическая гонка со свежей перепроверкой
// (зависшее без XACK сообщение пережило бы полный цикл recheck): попытки
// доставки исчерпываются в q:dead за минуты (T-025), а recheck-интервал
// измеряется месяцами (§3.3) — окно практически недостижимо.
//
// Учёт refcount (T-044/T-056): новые content_hash уже получили +1 от Put
// ContentSaver-а (T-056, до записи) — здесь только −1 удаляемым строкам:
// SwapRefTx(old, нулевой Hash) — примитив «новый нулевой → только −старому».
// Повтор доставки сохраняет баланс: повторный Put даёт лишний +1 (безопасная
// утечка места, T-056), DELETE той же строки — −1; уход в минус невозможен
// (охрана refcount > 0, расхождение — ErrRefIntegrity с откатом, T-044).
package httpworker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/netip"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"

	"crawler/internal/content"
	"crawler/internal/db"
	"crawler/internal/model"
	"crawler/internal/queue"
)

// SQL записи результата. Коды схемы — литералами, а не данными (стиль
// T-039); now() — время транзакции сервера БД: checked_at строк и
// last_checked_at домена — один момент (transaction_timestamp стабилен
// внутри транзакции).
var (
	// recordDomainSQL — итоговый статус эпизода HTTP (alive/no_http/
	// temp_error, §7), done, сброс lease и счётчика повторов (контракт
	// T-031). Охрана status <> deleted — §8/§13.1; UPDATE первым шагом
	// транзакции блокирует строку домена: конкурентные записи эпизода
	// сериализуются, решение об удалении принимается на актуальной
	// версии строки. Охраны check_state <> done нет — повторная доставка
	// перезаписывает результат (комментарий файла).
	recordDomainSQL = "UPDATE domains AS t " +
		"SET status = $3::int2, " +
		fmt.Sprintf("temp_attempts = CASE WHEN $3::int2 = %d THEN t.temp_attempts ELSE 0 END, ", int16(model.StatusTempError)) +
		fmt.Sprintf("check_state = %d, queued_at = NULL, last_checked_at = now() ", int16(model.CheckStateDone)) +
		fmt.Sprintf("WHERE t.zone_id = $1::int2 AND t.id = $2::int8 AND t.status <> %d", int16(model.StatusDeleted))

	// recordDeleteSQL — перезапись результата (§3.5): удаляются все строки
	// домена, RETURNING собирает content_hash для свопа ссылок (T-044).
	// Ведущие колонки PK (zone_id, domain_id) — партиционирование LIST по
	// zone_id и субпартиционирование HASH (T-005) режут поиск до листа.
	recordDeleteSQL = "DELETE FROM subdomain_results " +
		"WHERE zone_id = $1::int2 AND domain_id = $2::int8 " +
		"RETURNING content_hash"

	// recordInsertSQL — новые строки одним оператором: INSERT … SELECT
	// FROM unnest($n::type[]…) — каждая колонка один параметр-массив
	// (стиль db.Update, T-007), пачка ≤ 126 строк (субдомены ≤ 63 ×
	// варианты ≤ 2) не упирается в лимит протокола. COPY отклонён:
	// выигрыш копирования проявляется на тысячах строк, а unnest-INSERT —
	// один оператор транзакции с точным SQL-контрактом в юнит-тестах
	// (фантазировать с CopyFrom-источником поверх транзакции сложнее,
	// чем массивы). rendered = false (рендер — T-067), rules_version =
	// NULL (анализатор — T-068+), checked_at = now().
	recordInsertSQL = "INSERT INTO subdomain_results (" +
		"zone_id, domain_id, subdomain_id, variant, scheme, " +
		"http_status, final_url, alias_kind, alias_subdomain_id, " +
		"addrs, cname, tls, redirects, headers, cookies, " +
		"rendered, content_hash, error, response_ms, checked_at, rules_version) " +
		"SELECT z.zone_id, z.domain_id, z.subdomain_id, z.variant, z.scheme, " +
		"z.http_status, z.final_url, z.alias_kind, z.alias_subdomain_id, " +
		"z.addrs, z.cname, z.tls, z.redirects, z.headers, z.cookies, " +
		"false, z.content_hash, z.error, z.response_ms, now(), NULL " +
		"FROM unnest(" +
		"$1::int2[], $2::int8[], $3::int2[], $4::int2[], $5::int2[], " +
		"$6::int2[], $7::text[], $8::int2[], $9::int2[], " +
		"$10::bytea[], $11::text[], $12::jsonb[], $13::jsonb[], $14::jsonb[], $15::jsonb[], " +
		"$16::bytea[], $17::int2[], $18::int4[]) " +
		"AS z(zone_id, domain_id, subdomain_id, variant, scheme, " +
		"http_status, final_url, alias_kind, alias_subdomain_id, " +
		"addrs, cname, tls, redirects, headers, cookies, " +
		"content_hash, error, response_ms)"
)

// RecordDB — пул для записи результата: db.DB с транзакциями (стиль
// content.RefDB, ingest.CleanupDB). Запись результата и своп ссылок идут
// одной транзакцией (ТЗ 3.4); *pgxpool.Pool удовлетворяет интерфейсу.
type RecordDB interface {
	db.DB
	Begin(ctx context.Context) (pgx.Tx, error)
}

// Recorder — записывает результат HTTP-эпизода в subdomain_results,
// переводит домен в итоговый статус и публикует q:analyze (T-058), а при
// подключённой очереди рендера — q:render по SPA-записям с флагом
// render_pending при переполнении (T-066, renderq.go). Подключается
// сборкой сервиса в Options.OnResult — вслед за ContentSaver (T-056:
// новый контент уже имеет +1 refcount от Put) и до XACK (ТЗ 4.2).
// Потокобезопасен: состояние неизменно после New.
type Recorder struct {
	pool RecordDB
	q    queue.Queue
	// render — очередь рендера (T-066): nil у NewRecorder — этап рендера
	// отключён (q:render не публикуется, флаг не ставится); ставится
	// NewRenderRecorder.
	render *RenderQueue
	log    *slog.Logger
}

// NewRecorder строит записатель поверх пула БД и очереди (продакшн —
// *pgxpool.Pool и очередь под WatermarkPublisher, T-026). nil-зависимости
// — ошибка: тихая потеря результата/публикации незаметна для оператора.
func NewRecorder(pool RecordDB, q queue.Queue, log *slog.Logger) (*Recorder, error) {
	if pool == nil {
		return nil, errors.New("httpworker: запись результата: пул не задан")
	}
	if q == nil {
		return nil, errors.New("httpworker: запись результата: очередь не задана")
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Recorder{pool: pool, q: q, log: log}, nil
}

// RecordAndPublish записывает результат домена одной транзакцией (UPDATE
// domains + флаг render_pending при переполнении q:render, T-066 + DELETE
// старых строк + своп ссылок contents + INSERT новых, ТЗ 3.4/§5.5 п. 7)
// и после фиксации публикует q:analyze по одному Result на запись
// субдомен×вариант (формат T-024) и q:render по SPA-записям при
// свободной очереди (T-066). Возвращает false без ошибки, когда охрана
// отсекла запись (домен удалён — §8/§13.1, или строки нет): это успех,
// публикация не нужна. Ошибка — сбой любого шага: доставка q:http не
// подтверждается и вернётся повторной доставкой (Processing, T-025);
// повторная доставка перезапишет результат идемпотентно и заново решит
// вопрос рендера (флаг — CASE-free: ставится только при переполнении,
// снимает проход доработки renderq.go).
func (r *Recorder) RecordAndPublish(ctx context.Context, res *DomainResult) (bool, error) {
	if res == nil {
		return false, nil // не бывает (processDelivery); защита прямых вызовов
	}
	batch, err := newRecordBatch(res)
	if err != nil {
		return false, err
	}
	status := recordStatus(res)

	// T-066: SPA-ключи q:render (§5.5 п. 8) и решение о ёмкости — ДО
	// транзакции: при переполнении флаг render_pending фиксируется той же
	// транзакцией результата (§3.4), потому решение нужно до её открытия.
	// Проверка неблокирующая (HasCapacity, не WatermarkPublisher):
	// ожидание ёмкости задержало бы XACK — рендер не должен задерживать
	// остальные этапы (§5.6).
	var renderKeys []queue.Result
	renderPending := false
	if r.render != nil {
		renderKeys = renderSPAResults(res)
		if len(renderKeys) > 0 {
			free, err := r.render.HasCapacity(ctx)
			if err != nil {
				// XLEN недоступен — консервативно «места нет»: флаг
				// сохраняет задачи рендера, эпизод не задерживается;
				// настоящая недоступность Redis всё равно вернёт
				// доставку на публикации q:analyze ниже.
				r.log.Warn("ёмкость q:render не проверена — домен помечен render_pending",
					"domain", res.Name, "err", err)
				free = false
			}
			renderPending = !free
		}
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("httpworker: домен %s: запись результата: транзакция: %w", res.Name, err)
	}
	// Откат безусловно: после Commit — no-op (стиль T-044).
	defer func() { _ = tx.Rollback(ctx) }()

	// Итог домена первым шагом: блокирует строку и решает судьбу записи
	// (удалённому домену результат не пишется, §8).
	tag, err := tx.Exec(ctx, recordDomainSQL, res.ZoneID, res.DomainID, int16(status))
	if err != nil {
		return false, fmt.Errorf("httpworker: домен %s: запись результата: статус: %w", res.Name, err)
	}
	if tag.RowsAffected() == 0 {
		// Домен удалён во время проверки (§13.1) или строки нет —
		// штатные случаи, откат пустой транзакции.
		r.log.Debug("результат не записан — домен удалён или отсутствует",
			"domain", res.Name, "zone_id", res.ZoneID, "domain_id", res.DomainID)
		return false, nil
	}

	// Флаг render_pending — той же транзакцией (T-066, §5.6): решение
	// относится к этому результату (переполнение q:render в момент
	// записи), сбой фиксации откатывает и строки, и флаг; строка уже
	// заблокирована UPDATE выше — RowsAffected обязан быть 1.
	if renderPending {
		tag, err := tx.Exec(ctx, renderSetPendingSQL, res.ZoneID, res.DomainID)
		if err != nil {
			return false, fmt.Errorf("httpworker: домен %s: флаг render_pending: %w", res.Name, err)
		}
		if tag.RowsAffected() != 1 {
			return false, fmt.Errorf("httpworker: домен %s: флаг render_pending: затронуто %d строк", res.Name, tag.RowsAffected())
		}
	}

	// Перезапись (§3.5): старые строки домена уходят целиком, их ссылки
	// на контент снимаются здесь же — строка и счётчик не расходятся
	// (T-044).
	oldHashes, err := recordDeleteOld(ctx, tx, res.ZoneID, res.DomainID)
	if err != nil {
		return false, err
	}
	for _, h := range oldHashes {
		if err := content.SwapRefTx(ctx, tx, h, content.Hash{}); err != nil {
			return false, fmt.Errorf("httpworker: домен %s: %w", res.Name, err)
		}
	}

	if len(batch.zone) > 0 {
		tag, err := tx.Exec(ctx, recordInsertSQL, batch.args()...)
		if err != nil {
			return false, fmt.Errorf("httpworker: домен %s: запись результата: строки: %w", res.Name, err)
		}
		if int(tag.RowsAffected()) != len(batch.zone) {
			return false, fmt.Errorf("httpworker: домен %s: вставлено %d строк из %d", res.Name, tag.RowsAffected(), len(batch.zone))
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("httpworker: домен %s: запись результата: фиксация: %w", res.Name, err)
	}

	// Публикация q:analyze после записи (§5.5 п. 9): Result указывает на
	// строку — она обязана существовать к моменту потребления. Все записи
	// домена: анализ идёт по каждому результату субдомен×вариант, рендер
	// (T-067) позже заменит контент и опубликует заново — граница этапов,
	// не фильтр здесь.
	for i := range batch.pub {
		if _, err := r.q.Publish(ctx, queue.StreamAnalyze, batch.pub[i]); err != nil {
			return true, fmt.Errorf("httpworker: домен %s: публикация q:analyze (запись %d/%d): %w",
				res.Name, i+1, len(batch.pub), err)
		}
	}

	// q:render по SPA-записям (T-066, §5.5 п. 8): после фиксации — как и
	// q:analyze, Result указывает на строку. Только при свободной очереди:
	// переполнение уже закрыто флагом в транзакции. Публикация мимо
	// WatermarkPublisher (T-026) — ёмкость проверена выше, ожидание здесь
	// задержало бы XACK (§5.6).
	if len(renderKeys) > 0 && !renderPending {
		if err := r.render.Publish(ctx, renderKeys); err != nil {
			return true, fmt.Errorf("httpworker: домен %s: %w", res.Name, err)
		}
	}

	r.log.Debug("HTTP-результат записан",
		"domain", res.Name, "status", status.String(), "records", len(batch.zone),
		"swapped_refs", len(oldHashes),
		"render_pending", renderPending, "q_render", len(renderKeys))
	return true, nil
}

// recordDeleteOld удаляет старые строки домена и возвращает агрегированные
// content_hash для свопа ссылок (сортировка — детерминированный порядок
// операторов; кратность хэша = числу ссылающихся строк — повторное
// вхождение даёт повторное −1, SwapRefTx вызывается на каждое).
func recordDeleteOld(ctx context.Context, tx pgx.Tx, zoneID int16, domainID int64) ([]content.Hash, error) {
	rows, err := tx.Query(ctx, recordDeleteSQL, zoneID, domainID)
	if err != nil {
		return nil, fmt.Errorf("httpworker: домен %d/%d: удаление старых строк: %w", zoneID, domainID, err)
	}
	defer rows.Close()

	counts := make(map[content.Hash]int)
	for rows.Next() {
		var hash []byte
		if err := rows.Scan(&hash); err != nil {
			return nil, fmt.Errorf("httpworker: домен %d/%d: удаление старых строк: чтение: %w", zoneID, domainID, err)
		}
		if hash == nil {
			continue // строка без контента — снимать нечего
		}
		var h content.Hash
		copy(h[:], hash)
		counts[h]++
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("httpworker: домен %d/%d: удаление старых строк: %w", zoneID, domainID, err)
	}

	out := make([]content.Hash, 0, len(counts))
	for h, n := range counts {
		for i := 0; i < n; i++ {
			out = append(out, h)
		}
	}
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i][:], out[j][:]) < 0 })
	return out, nil
}

// recordStatus — итоговый статус домена HTTP-эпизода (§5.5 п. 9, §7).
func recordStatus(res *DomainResult) model.Status {
	switch {
	case res.HasRecords():
		return model.StatusAlive
	case res.AnyTimeout():
		return model.StatusTempError
	default:
		return model.StatusNoHTTP
	}
}

// recordBatch — unnest-пачка новых строк: параллельные массивы колонок
// (одна колонка — один параметр recordInsertSQL, стиль T-007) и сообщения
// q:analyze (по одному на запись). NULL-колонки — nil-элементы массивов
// (pgx кодирует их в NULL); jsonb — текст JSON (text → jsonb приводится
// на сервере; bytea в jsonb PostgreSQL не приводит).
type recordBatch struct {
	zone       []int16
	domain     []int64
	subdomain  []int16
	variant    []int16
	scheme     []int16
	httpStatus []*int16
	finalURL   []*string
	aliasKind  []int16
	aliasSub   []*int16
	addrs      [][]byte
	cname      []*string
	tls        []*string
	redirects  []*string
	headers    []*string
	cookies    []*string
	hash       [][]byte
	errCode    []*int16
	responseMS []*int32

	pub []queue.Result
}

func (b *recordBatch) args() []any {
	return []any{
		b.zone, b.domain, b.subdomain, b.variant, b.scheme,
		b.httpStatus, b.finalURL, b.aliasKind, b.aliasSub,
		b.addrs, b.cname, b.tls, b.redirects, b.headers, b.cookies,
		b.hash, b.errCode, b.responseMS,
	}
}

// newRecordBatch собирает пачку из результата домена и валидирует записи
// (коды модели, уникальность PK субдомен×вариант): ошибка — до открытия
// транзакции, БД не трогается.
func newRecordBatch(res *DomainResult) (*recordBatch, error) {
	if res.ZoneID <= 0 || res.DomainID <= 0 {
		return nil, fmt.Errorf("httpworker: запись результата: зона %d, домен %d — идентификаторы положительны", res.ZoneID, res.DomainID)
	}
	n := res.RecordCount()
	b := &recordBatch{
		zone: make([]int16, 0, n), domain: make([]int64, 0, n),
		subdomain: make([]int16, 0, n), variant: make([]int16, 0, n),
		scheme: make([]int16, 0, n), httpStatus: make([]*int16, 0, n),
		finalURL: make([]*string, 0, n), aliasKind: make([]int16, 0, n),
		aliasSub: make([]*int16, 0, n), addrs: make([][]byte, 0, n),
		cname: make([]*string, 0, n), tls: make([]*string, 0, n),
		redirects: make([]*string, 0, n), headers: make([]*string, 0, n),
		cookies: make([]*string, 0, n), hash: make([][]byte, 0, n),
		errCode: make([]*int16, 0, n), responseMS: make([]*int32, 0, n),
		pub: make([]queue.Result, 0, n),
	}
	seen := make(map[recordKey]struct{}, n)
	for i := range res.Subdomains {
		sub := &res.Subdomains[i]
		addrs := recordPackAddrs(sub.Addrs)
		for j := range sub.Records {
			rec := &sub.Records[j]
			key := recordKey{sub: sub.SubdomainID, variant: rec.Variant}
			if _, dup := seen[key]; dup {
				return nil, fmt.Errorf("httpworker: домен %s: субдомен %d, вариант %d: дубликат ключа subdomain_results", res.Name, sub.SubdomainID, int16(rec.Variant))
			}
			seen[key] = struct{}{}
			if err := recordValidateRow(res.Name, sub, rec); err != nil {
				return nil, err
			}

			var httpStatus *int16
			if rec.HTTPStatus > 0 {
				v := int16(rec.HTTPStatus)
				httpStatus = &v
			}
			var finalURL *string
			if rec.FinalURL != "" {
				finalURL = &rec.FinalURL
			}
			var cname *string
			if sub.CNAME != "" {
				cname = &sub.CNAME
			}
			tls, err := recordJSON(rec.TLS != nil, rec.TLS)
			if err != nil {
				return nil, fmt.Errorf("httpworker: домен %s: субдомен %s: TLS: %w", res.Name, sub.Hostname, err)
			}
			redirects, err := recordJSON(len(rec.Redirects) > 0, rec.Redirects)
			if err != nil {
				return nil, fmt.Errorf("httpworker: домен %s: субдомен %s: цепочка редиректов: %w", res.Name, sub.Hostname, err)
			}
			headers, err := recordJSON(len(rec.Header) > 0, rec.Header)
			if err != nil {
				return nil, fmt.Errorf("httpworker: домен %s: субдомен %s: заголовки: %w", res.Name, sub.Hostname, err)
			}
			cookies, err := recordCookies(rec.Cookies)
			if err != nil {
				return nil, fmt.Errorf("httpworker: домен %s: субдомен %s: cookies: %w", res.Name, sub.Hostname, err)
			}
			var errCode *int16
			if rec.ErrClass != ErrNone {
				v := int16(rec.ErrClass)
				errCode = &v
			}
			var hash []byte
			if rec.ContentHash != nil {
				hash = rec.ContentHash[:]
			}
			responseMS := int32(rec.ResponseMS)

			b.zone = append(b.zone, res.ZoneID)
			b.domain = append(b.domain, res.DomainID)
			b.subdomain = append(b.subdomain, sub.SubdomainID)
			b.variant = append(b.variant, int16(rec.Variant))
			b.scheme = append(b.scheme, int16(rec.Scheme))
			b.httpStatus = append(b.httpStatus, httpStatus)
			b.finalURL = append(b.finalURL, finalURL)
			b.aliasKind = append(b.aliasKind, int16(rec.AliasKind))
			b.aliasSub = append(b.aliasSub, rec.AliasSubdomainID)
			b.addrs = append(b.addrs, addrs)
			b.cname = append(b.cname, cname)
			b.tls = append(b.tls, tls)
			b.redirects = append(b.redirects, redirects)
			b.headers = append(b.headers, headers)
			b.cookies = append(b.cookies, cookies)
			b.hash = append(b.hash, hash)
			b.errCode = append(b.errCode, errCode)
			b.responseMS = append(b.responseMS, &responseMS)
			b.pub = append(b.pub, queue.Result{
				ZoneID:      res.ZoneID,
				DomainID:    res.DomainID,
				SubdomainID: sub.SubdomainID,
				Variant:     rec.Variant,
				Name:        res.Name,
			})
		}
	}
	return b, nil
}

// recordKey — ключ уникальности PK subdomain_results.
type recordKey struct {
	sub     int16
	variant model.Variant
}

// recordValidateRow — инварианты записи (коды модели §6.1; контракт
// алиасов T-055: вид связи ⟺ ссылка; ContentHash либо nil, либо полный
// 16-байтный хэш — типизирован content.Hash, проверка длины — страховка
// прямых вызовов). Обрезанному телу хэша для хранения не полагается
// (T-056) — ContentNotStored ⇒ hash nil.
func recordValidateRow(name string, sub *SubdomainResult, rec *Record) error {
	if !rec.Variant.Valid() {
		return fmt.Errorf("httpworker: домен %s: субдомен %d: вариант %d", name, sub.SubdomainID, int16(rec.Variant))
	}
	if !rec.Scheme.Valid() {
		return fmt.Errorf("httpworker: домен %s: субдомен %d: схема %d", name, sub.SubdomainID, int16(rec.Scheme))
	}
	if !rec.AliasKind.Valid() {
		return fmt.Errorf("httpworker: домен %s: субдомен %d: вид алиаса %d", name, sub.SubdomainID, int16(rec.AliasKind))
	}
	if (rec.AliasKind == model.AliasNone) != (rec.AliasSubdomainID == nil) {
		return fmt.Errorf("httpworker: домен %s: субдомен %d: alias_kind %d ⟺ alias_subdomain_id (контракт T-055)", name, sub.SubdomainID, int16(rec.AliasKind))
	}
	if rec.ContentHash != nil && len(*rec.ContentHash) != content.HashLen {
		return fmt.Errorf("httpworker: домен %s: субдомен %d: content_hash %d байт (нужно %d)", name, sub.SubdomainID, len(*rec.ContentHash), content.HashLen)
	}
	if rec.ContentNotStored && rec.ContentHash != nil {
		return fmt.Errorf("httpworker: домен %s: субдомен %d: ContentNotStored при живом content_hash (контракт T-056)", name, sub.SubdomainID)
	}
	return nil
}

// recordJSON сериализует значение в jsonb-колонку, когда оно есть
// (some — признак); nil-элемент массива → NULL. Значения с JSON-тегами:
// TLSInfo (T-050), RedirectStep (T-048); http.Header — канонические имена
// заголовков как есть.
func recordJSON(some bool, value any) (*string, error) {
	if !some {
		return nil, nil
	}
	b, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	s := string(b)
	return &s, nil
}

// recordCookie — cookie итогового ответа для subdomain_results.cookies
// (jsonb, ТЗ 5.5 п. 7): имя и значение — цели правил анализатора
// (ТЗ 5.8: cookie имя/значение), атрибуты — домен/путь/срок — про запас;
// теги snake_case (стиль TLSInfo).
type recordCookie struct {
	Name     string `json:"name"`
	Value    string `json:"value,omitempty"`
	Path     string `json:"path,omitempty"`
	Domain   string `json:"domain,omitempty"`
	Expires  string `json:"expires,omitempty"` // RFC 3339, UTC
	MaxAge   int    `json:"max_age,omitempty"`
	Secure   bool   `json:"secure,omitempty"`
	HTTPOnly bool   `json:"http_only,omitempty"`
	SameSite string `json:"same_site,omitempty"`
}

// recordSameSite — имя атрибута SameSite (строковые коды jsonb): default
// и нулевое значение — атрибут не выставлен (пустая строка, omitempty).
func recordSameSite(s http.SameSite) string {
	switch s {
	case http.SameSiteLaxMode:
		return "lax"
	case http.SameSiteStrictMode:
		return "strict"
	case http.SameSiteNoneMode:
		return "none"
	default:
		return ""
	}
}

// recordCookies сериализует Set-Cookie итогового ответа (ТЗ 5.5 п. 7):
// nil при пустом наборе → NULL.
func recordCookies(cookies []*http.Cookie) (*string, error) {
	if len(cookies) == 0 {
		return nil, nil
	}
	out := make([]recordCookie, 0, len(cookies))
	for _, c := range cookies {
		rc := recordCookie{
			Name:     c.Name,
			Value:    c.Value,
			Path:     c.Path,
			Domain:   c.Domain,
			MaxAge:   c.MaxAge,
			Secure:   c.Secure,
			HTTPOnly: c.HttpOnly,
			SameSite: recordSameSite(c.SameSite),
		}
		if !c.Expires.IsZero() {
			rc.Expires = c.Expires.UTC().Format(time.RFC3339)
		}
		out = append(out, rc)
	}
	return recordJSON(true, out)
}

// recordPackAddrs упаковывает адреса субдомена в bytea subdomain_results.
// addrs (§6.1 «упакованные A/AAAA», формат — этап, кладущий строку):
// 16 октетов на адрес, IPv4 — в v4-mapped форме (::ffff:a.b.c.d,
// netip.Addr.As16) — состав смешанных A/AAAA не двусмысленен (сырые 4/16
// октетов давали бы коллизии длин), длина bytea всегда кратна 16. Порядок
// — задачи q:http (A, затем AAAA, порядок ответа DNS-этапа, T-037).
func recordPackAddrs(addrs []netip.Addr) []byte {
	if len(addrs) == 0 {
		return nil
	}
	out := make([]byte, 0, 16*len(addrs))
	for _, a := range addrs {
		b16 := a.Unmap().As16()
		out = append(out, b16[:]...)
	}
	return out
}

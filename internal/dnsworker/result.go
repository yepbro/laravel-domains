// Запись результата DNS-эпизода и рабочий цикл dns-worker (T-039; ТЗ 5.4
// пп. 4–5, §7, 6.1, 6.2, 3.4). Файл замыкает конвейер этапа: эпизод
// (CheckApex T-036 → CheckSubdomains T-037 → DetectWildcard T-038)
// превращается в запись domains и — при найденных адресах — в задачу
// q:http (формат T-024), а цикл Run собирает всё это на потреблении
// пачек q:dns (T-024/T-025). Сборка сервиса — cmd/dns-worker.
//
// Статусы по §7 (решения зафиксированы по тексту ТЗ):
//
//   - NXDOMAIN апекса → status = not_delegated, эпизод done (§5.4 п. 1,
//     RFC 8020): субдомены не проверялись, маска обнуляется — хранится
//     только последний результат (§3.5);
//   - SERVFAIL/таймаут апекса → status = temp_error, done: повтор
//     назначит планировщик (T-031 по temp_error_retry_after);
//   - адреса есть (маска без wildcard-исключений непуста, §5.4 п. 5) →
//     задача q:http, done; терминальный статус НЕ пишется: alive/no_http
//     — итог HTTP-этапа (§6.2), домен сохраняет прежний статус (для
//     первой проверки — new) до его завершения. Аналог T-030: статус
//     хранит итог прошлой проверки, check_state — состояние цикла;
//   - адресов нет → status = delegated_no_address, done («есть NS/ответ,
//     но нет A/AAAA у ни одного субдомена», §5.4 п. 5). Сюда же угловой
//     случай «резолвятся только wildcard-синтезированные имена»: все
//     DNS-живые субдомены объяснены wildcard-ом (кроме апекса/www,
//     T-038), публиковать пустую q:http нельзя (контракт T-024) —
//     HTTP-проверимых имён нет, что по смыслу и есть «нет адресов»;
//     dns_mask при этом честно хранит резолвящиеся биты (§5.4 п. 4:
//     бит — «резолвящийся субдомен», wildcard-фильтр исключает из
//     HTTP-проверки, не из маски);
//   - сбой резолвера по субдомену (транспорт после ретраев, SERVFAIL,
//     иные коды, кроме NOERROR/NXDOMAIN) при найденных адресах — эпизод
//     завершается задачей q:http по частичному итогу (T-037: итог по
//     каждому субдомену отдельно; потерянное имя покроет регулярная
//     перепроверка T-030), без адресов — temp_error: классифицировать
//     «нет адресов» по молчащему резолверу нельзя (ложный терминальный
//     статус), повтор T-031 перепроверит эпизод. Дефекты самой зоны —
//     цикл и превышение глубины CNAME (ErrSubdomCNAMELoop/Depth) — сбоям
//     не ровня: резолвер ответил, сломано у домена; итог субдомена
//     «адресов нет» без temp_error (sentinel-ошибки разбираются
//     errors.Is, T-037).
//
// Порядок «публикация q:http → запись done» (§3.4, в духе T-028:
// PostgreSQL — источник истины, done — точка фиксации эпизода):
//
//   - задача q:http публикуется ДО перевода домена в done. Сбой записи
//     после публикации оставляет домен queued и доставку q:dns без XACK
//     (Processing, T-025): повторная доставка повторит эпизод, но задача
//     не теряется. Обратный порядок при сбое публикации оставлял бы
//     домен done без задачи q:http навсегда (HTTP-этап не начнётся,
//     фильтр актуальности отбрасывает done-строки, чинила бы только
//     перепроверка T-030 — полгода) — отвергнут ревью DNS-04 и
//     перепроверен для Q-02: RecoverLoop возвращает только зависшие
//     доставки, состояние строки ему не изменить;
//   - единица повтора сужена до домена (DNS-04, T-108): сразу после
//     успешной публикации q:http эпизод ставит staging-пометку
//     http_published (бит 2 domains.flags, модель model.Flags — как
//     render_pending T-066; без миграции: ТЗ 6.1 оставляет биты flags
//     открытыми, «…»). Повторная доставка пачки читает бит фильтром
//     актуальности и НЕ публикует q:http повторно — только доходит до
//     записи (DNS-эпизод повторяется: результат не хранился нигде,
//     §3.5, а задачи q:http на стороне потребителя дедупликации не
//     имеет — дубль = полная повторная HTTP-проверка). До T-108 один
//     сбой записи стоил до redis.max_attempts × |пачка| повторных
//     эпизодов и дублей q:http по опубликовавшим доменам; теперь дубль
//     возможен только в окне между XADD и постановкой бита (краш воркера
//     или отказ БД в одном UPDATE) и в межэпизодных гонках lease
//     (существовали и до T-108);
//   - снятие бита: записью результата (тот же UPDATE — done ⇒ бит чист)
//     и постановкой планировщика (pending → queued, T-028: каждый
//     новый эпизод начинается с чистого бита — единая точка сброса
//     закрывает и lease-возврат T-029, и воскрешение T-018, и
//     перепроверку T-030 без правки каждого прохода). На done/pending
//     строке бит может остаться выставленным до следующей постановки —
//     читает его только фильтр пачки (done отсечён), значения вне
//     эпизода бит не имеет;
//   - постановка бита — non-fatal: отказ UPDATE после публикации не
//     роняет эпизод (запись результата важнее), повторная доставка в
//     этом случае публикует дубль — цена окна, а не потеря;
//   - из инварианта «done ⇒ задача опубликована» фильтр актуальности
//     выводит идемпотентность: повторная доставка пачки обрабатывает
//     только недоработанные домены (не done и не deleted), уже
//     записанные пропускаются без DNS-обмена и без дубликата q:http.
//
// Wildcard-детект (T-038) при сбое не роняет эпизод: продолжение без
// фильтрации — несколько лишних HTTP-запросов к заглушке; temp_error
// для домена не нужен (апекс уже классифицирован, T-036).

package dnsworker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"crawler/internal/db"
	"crawler/internal/dnsclient"
	"crawler/internal/metrics"
	"crawler/internal/model"
	"crawler/internal/queue"
)

// SQL записи результата. Один оператор на домен (стиль охран T-029/
// T-030): охраны перепроверяют состояние строки на момент блокировки —
// удалённый во время проверки домен не перезаписывается (§13.1, §8:
// «результат не записывается (проверка статуса при записи)»), уже
// записанный результат не затирается повторной доставкой (идемпотентность
// ТЗ 3.4). Коды схемы — литералами, а не данными (стиль dispatchZoneSQL,
// T-027); last_checked_at = now() — время сервера БД (T-028), колонка
// date получает дату приведением присваивания.
//
// temp_attempts (T-031): счётчик повторов сбрасывается в 0 на каждом
// не-temp_error итоге — новый эпизод временной ошибки начинается с чистого
// счётчика (контракт T-031 возложен на DNS-этап); temp_error сохраняет
// накопленное — наращивает планировщик при назначении повтора.
//
// queued_at = NULL завершает lease вместе с эпизодом: ненулевая отметка
// остаётся только у queued-строк (инвариант T-018/T-029/T-030).
//
// flags: оба оператора снимают бит http_published (T-108) — done-строка
// всегда чиста от staging-пометки; прочие биты (is_idn, render_pending)
// сохраняются знаковой маской ~бита (побитовые операторы PG повышают
// smallint до int — возврат ::int2, стиль renderq.go T-066).
var (
	// resultUpdateStatusSQL — терминальный статус DNS-эпизода
	// (not_delegated / temp_error / delegated_no_address).
	resultUpdateStatusSQL = "UPDATE domains AS t " +
		"SET dns_mask = $3::int8, status = $4::int2, " +
		fmt.Sprintf("temp_attempts = CASE WHEN $4::int2 = %d THEN t.temp_attempts ELSE 0 END, ", int16(model.StatusTempError)) +
		fmt.Sprintf("flags = (t.flags & %d)::int2, ", int16(^int16(model.FlagHTTPPublished))) +
		fmt.Sprintf("check_state = %d, queued_at = NULL, last_checked_at = now() ", int16(model.CheckStateDone)) +
		fmt.Sprintf("WHERE t.zone_id = $1::int2 AND t.id = $2::int8 AND t.status <> %d AND t.check_state <> %d",
			int16(model.StatusDeleted), int16(model.CheckStateDone))

	// resultUpdateKeepStatusSQL — ветка «есть адреса»: статус не пишется
	// (итог поставит HTTP-этап, §7), temp_attempts сбрасывается — эпизод
	// успешен.
	resultUpdateKeepStatusSQL = "UPDATE domains AS t " +
		"SET dns_mask = $3::int8, temp_attempts = 0, " +
		fmt.Sprintf("flags = (t.flags & %d)::int2, ", int16(^int16(model.FlagHTTPPublished))) +
		fmt.Sprintf("check_state = %d, queued_at = NULL, last_checked_at = now() ", int16(model.CheckStateDone)) +
		fmt.Sprintf("WHERE t.zone_id = $1::int2 AND t.id = $2::int8 AND t.status <> %d AND t.check_state <> %d",
			int16(model.StatusDeleted), int16(model.CheckStateDone))

	// resultMarkPublishedSQL — staging-пометка публикации q:http (T-108,
	// DNS-04): бит http_published ставится сразу после успешного XADD и
	// до записи результата — повторная доставка пачки по биту отличит
	// «опубликовано, не записано» от «не доходило» и не станет публиковать
	// дубль. Охраны — как у записи результата: done-строка пометки не
	// получает (эпизод уже зафиксирован), deleted — вне цикла (§13.1);
	// строки в pending (lease вернул посреди эпизода) помечаются — задача
	// опубликована независимо от аренды. OR сохраняет прочие биты flags.
	resultMarkPublishedSQL = "UPDATE domains AS t " +
		fmt.Sprintf("SET flags = (t.flags | %d)::int2 ", int16(model.FlagHTTPPublished)) +
		fmt.Sprintf("WHERE t.zone_id = $1::int2 AND t.id = $2::int8 AND t.status <> %d AND t.check_state <> %d",
			int16(model.StatusDeleted), int16(model.CheckStateDone))

	// resultInvalidSQL — терминальный диагноз ядовитой строки (T-109,
	// остаток DNS-03): статус invalid для домена, чья проверка
	// невозможна детерминированно (пустое имя, ErrEmptyDomain). Запись
	// завершает lease и выводит строку из бесконечного цикла queued →
	// lease-возврат → dispatch: перепроверка (recheck, T-030) invalid не
	// возвращает — строки не переиздаются. dns_mask не пишется: маска —
	// факт DNS-эпизода, которого у мусорной строки нет; temp_attempts
	// сбрасывается (повторов не будет); бит http_published снимается —
	// done-строка чиста от staging-пометки (T-108). Охраны — как у
	// записи результата: deleted не перезаписывается (§13.1), повторная
	// доставка уже зафиксированного диагноза идемпотентна (ТЗ 3.4).
	resultInvalidSQL = "UPDATE domains AS t " +
		fmt.Sprintf("SET status = %d::int2, temp_attempts = 0, ", int16(model.StatusInvalid)) +
		fmt.Sprintf("flags = (t.flags & %d)::int2, ", int16(^int16(model.FlagHTTPPublished))) +
		fmt.Sprintf("check_state = %d, queued_at = NULL, last_checked_at = now() ", int16(model.CheckStateDone)) +
		fmt.Sprintf("WHERE t.zone_id = $1::int2 AND t.id = $2::int8 AND t.status <> %d AND t.check_state <> %d",
			int16(model.StatusDeleted), int16(model.CheckStateDone))

	// resultPendingSQL — фильтр актуальности пачки q:dns: остаются домены
	// ещё не записанные (check_state <> done — повторная доставка,
	// идемпотентность) и не удалённые (§13.1); flags — staging-пометка
	// публикации q:http (T-108): бит говорит повторной доставке пропустить
	// публикацию. unnest WITH ORDINALITY сохраняет порядок пачки
	// (чередование зон T-027) детерминированным.
	resultPendingSQL = "SELECT u.zone_id, u.id, d.flags " +
		"FROM unnest($1::int2[], $2::int8[]) WITH ORDINALITY AS u(zone_id, id, ord) " +
		"JOIN domains AS d ON d.zone_id = u.zone_id AND d.id = u.id " +
		fmt.Sprintf("WHERE d.status <> %d AND d.check_state <> %d ", int16(model.StatusDeleted), int16(model.CheckStateDone)) +
		"ORDER BY u.ord"

	// subdomainsSelectSQL — справочник проверяемых субдоменов (ТЗ 5.10).
	subdomainsSelectSQL = "SELECT id, label, enabled FROM subdomains ORDER BY id"
)

// ErrSubdomainsEmpty — справочник subdomains пуст (сбой миграции/чистка):
// состояние детерминировано, повторная доставка его не лечит — цикл
// воркера подтверждает пачку с Warn (домены вернёт lease-проход
// T-029/T-030 после починки справочника), а не крутит повторы до q:dead
// (DNS-03).
var ErrSubdomainsEmpty = errors.New("dnsworker: справочник subdomains пуст — нужен хотя бы апекс (ТЗ 5.10)")

// EpisodeResult — решение DNS-эпизода одного домена: что писать в
// domains и что публиковать в q:http (ТЗ 5.4 пп. 4–5, §7). Строится
// чистой функцией DecideEpisode из результатов T-036/037/038.
type EpisodeResult struct {
	// SetStatus — писать ли domains.status: терминальные ветки DNS-этапа
	// (not_delegated/temp_error/delegated_no_address); false — есть
	// адреса, итог за HTTP-этапом (§7).
	SetStatus bool
	// Status — статус терминальной ветки (валиден при SetStatus).
	Status model.Status
	// Mask — domains.dns_mask: биты резолвящихся субдоменов этого эпизода
	// (§5.4 п. 4), включая wildcard-синтезированные (T-038 исключает их
	// из HTTP-проверки, не из маски).
	Mask model.DNSMask
	// HTTPSubdomains — DNS-живые субдомены задачи q:http (T-024): адреса
	// A/AAAA и CNAME (§5.4 п. 4), wildcard-исключённые — кроме апекса и
	// www, которые не помечаются никогда (T-038). Пусто — задачи нет.
	HTTPSubdomains []queue.HTTPSubdomain
	// TempFailed — были ли у субдоменов сбои резолвера (транспорт,
	// SERVFAIL/иные коды): диагностика для логов; на решение влияет только
	// при пустом HTTP-наборе (temp_error вместо delegated_no_address).
	TempFailed bool
}

// HasHTTPTask сообщает, есть ли адреса для HTTP-этапа (§5.4 п. 5: «есть
// адреса → задача в q:http») — по набору после wildcard-фильтра.
func (r EpisodeResult) HasHTTPTask() bool { return len(r.HTTPSubdomains) > 0 }

// DecideEpisode сводит результаты шагов эпизода к решению записи (ТЗ §7).
// Чистая функция — без БД и очереди; apex обязателен, отчёт субдоменов —
// для ветвей делегирования (Stop-ветки апекса субдомены не проверяют,
// RFC 8020/T-036). Правила статусов — в комментарии файла.
func DecideEpisode(apex *ApexResult, rep *SubdomReport, wild WildcardResult) (EpisodeResult, error) {
	if apex == nil {
		return EpisodeResult{}, errors.New("dnsworker: решение эпизода: нет результата апекса (T-036)")
	}
	var out EpisodeResult

	// Терминальные ветки апекса: NXDOMAIN (not_delegated) и
	// SERVFAIL/таймаут (temp_error) — субдомены не проверялись, маска
	// эпизода пуста (§3.5 — только последний результат).
	if status, terminal := apex.Status.DomainStatus(); terminal {
		out.SetStatus = true
		out.Status = status
		out.TempFailed = status == model.StatusTempError
		return out, nil
	}

	if rep == nil {
		return EpisodeResult{}, errors.New("dnsworker: решение эпизода: нет отчёта субдоменов (T-037)")
	}
	out.Mask = rep.Mask

	temp := false
	for _, r := range rep.Results {
		// Сбой резолвера: транспорт (Err) либо код вне {NOERROR, NXDOMAIN}
		// — NXDOMAIN субдомена авторитетен (имени нет, RFC 8020),
		// в том числе CNAME→NXDOMAIN (T-037). Дефекты цепочки CNAME
		// (ErrSubdomCNAMELoop/Depth, sentinel-ошибки T-037) — не сбой
		// резолвера: резолвер ответил исправно, зона сломана постоянно —
		// повтор эпизода не изменит исход, итог субдомена «адресов нет»,
		// а не temp_error (три полных повтора T-031 здесь — лишняя работа
		// и шумный статус).
		if r.Err != nil {
			if !errors.Is(r.Err, ErrSubdomCNAMELoop) && !errors.Is(r.Err, ErrSubdomCNAMEDepth) {
				temp = true
			}
		} else if r.RCode != dnsclient.RCodeNOERROR && r.RCode != dnsclient.RCodeNXDOMAIN {
			temp = true
		}
		if !r.HasAddrs() || wild.IsWildcard(r.ID) {
			continue // не DNS-живой или объяснён wildcard-ом (T-038)
		}
		out.HTTPSubdomains = append(out.HTTPSubdomains, queue.HTTPSubdomain{
			SubdomainID: int16(r.ID),
			Label:       r.Label,
			Addrs:       resultAddrStrings(r),
			CNAME:       resultCNAME(r.CNAME()), // прямой псевдоним, presentation-форма
		})
	}
	out.TempFailed = temp

	if out.HasHTTPTask() {
		// Есть адреса → q:http; статус не пишется (HTTP-этап решит,
		// §7). Частичный сбой субдоменов задачу не рушит (T-037).
		return out, nil
	}
	out.SetStatus = true
	if temp {
		out.Status = model.StatusTempError
	} else {
		out.Status = model.StatusDelegatedNoAddress
	}
	return out, nil
}

// resultAddrStrings — строковые адреса субдомена для задачи q:http
// (queue.HTTPSubdomain.Addrs, парсинг netip проверяет очередь, T-024):
// сначала A, затем AAAA, в порядке записей ответа (T-037).
func resultAddrStrings(r SubdomResult) []string {
	addrs := make([]string, 0, len(r.A)+len(r.AAAA))
	for _, a := range r.A {
		addrs = append(addrs, a.String())
	}
	for _, a := range r.AAAA {
		addrs = append(addrs, a.String())
	}
	return addrs
}

// resultCNAME — цель CNAME для задачи q:http без завершающей точки:
// имена в сообщениях очереди хранятся presentation-формой без корневой
// точки (как DNSDomain.Name, ТЗ 5.2), иначе предельный псевдоним
// 253 октета + точка не прошёл бы валидацию длины очереди (T-024).
func resultCNAME(c string) string {
	return strings.TrimSuffix(c, ".")
}

// RecordEpisode записывает результат DNS-эпизода одного домена (ТЗ 5.4
// п. 4) одним оператором UPDATE с охраной актуальности: удалённый во
// время проверки домен не перезаписывается (§13.1), уже записанный
// результат повторная доставка не трогает (идемпотентность, ТЗ 3.4 —
// XACK только после записи, T-025); тем же оператором снимается бит
// http_published (T-108): done — точка, где staging-пометка публикации
// выполнила своё назначение. Возвращает false, когда охраны отсекли
// строку (done/deleted) — это успех, а не ошибка.
func RecordEpisode(ctx context.Context, pool db.DB, zoneID int16, domainID int64, res EpisodeResult) (bool, error) {
	if pool == nil {
		return false, errors.New("dnsworker: запись результата: пул не задан")
	}
	if res.SetStatus && !res.Status.Valid() {
		return false, fmt.Errorf("dnsworker: запись результата домена %d/%d: недопустимый статус %d", zoneID, domainID, int16(res.Status))
	}

	var (
		tag pgconn.CommandTag
		err error
	)
	if res.SetStatus {
		tag, err = pool.Exec(ctx, resultUpdateStatusSQL, zoneID, domainID, int64(res.Mask), int16(res.Status))
	} else {
		tag, err = pool.Exec(ctx, resultUpdateKeepStatusSQL, zoneID, domainID, int64(res.Mask))
	}
	if err != nil {
		return false, fmt.Errorf("dnsworker: запись результата домена %d/%d: %w", zoneID, domainID, err)
	}
	return tag.RowsAffected() > 0, nil
}

// MarkHTTPPublished ставит бит http_published — staging-пометку «задача
// q:http этого эпизода опубликована» (T-108). Вызывается строго после
// успешной публикации и до записи результата: повторная доставка пачки
// по биту пропустит публикацию (нет дубля q:http) и доидёт до записи.
// Возвращает false, когда охраны отсекли строку (done/deleted) — успех.
func MarkHTTPPublished(ctx context.Context, pool db.DB, zoneID int16, domainID int64) (bool, error) {
	if pool == nil {
		return false, errors.New("dnsworker: пометка публикации: пул не задан")
	}
	tag, err := pool.Exec(ctx, resultMarkPublishedSQL, zoneID, domainID)
	if err != nil {
		return false, fmt.Errorf("dnsworker: пометка публикации домена %d/%d: %w", zoneID, domainID, err)
	}
	return tag.RowsAffected() > 0, nil
}

// RecordInvalid фиксирует терминальный диагноз ядовитой строки — статус
// invalid (T-109): домен, чья проверка невозможна детерминированно
// (пустое имя, ErrEmptyDomain), покидает цикл переиздания вместо
// бесконечного lease-возврата. Оператор resultInvalidSQL с охранами
// актуальности: удалённый домен не перезаписывается (§13.1), повторная
// доставка зафиксированного диагноза — no-op (идемпотентность, ТЗ 3.4).
// Возвращает false, когда охраны отсекли строку (done/deleted) — это
// успех, а не ошибка.
func RecordInvalid(ctx context.Context, pool db.DB, zoneID int16, domainID int64) (bool, error) {
	if pool == nil {
		return false, errors.New("dnsworker: диагноз invalid: пул не задан")
	}
	tag, err := pool.Exec(ctx, resultInvalidSQL, zoneID, domainID)
	if err != nil {
		return false, fmt.Errorf("dnsworker: диагноз invalid домена %d/%d: %w", zoneID, domainID, err)
	}
	return tag.RowsAffected() > 0, nil
}

// ProcessDomain — DNS-эпизод одного домена целиком (ТЗ 5.4, §7): апекс
// (T-036) → субдомены и wildcard-детект (T-037/T-038, только для ветвей
// делегирования) → решение (DecideEpisode) → публикация q:http → запись
// результата (RecordEpisode). Порядок «публикация → запись» и цена
// повтора при сбое записи — в комментарии файла; ошибка любого шага —
// ошибка эпизода: домен остаётся без записи, доставка q:dns не
// подтверждается и повторится (Processing, T-025).
//
// dom — домен пачки q:dns с состоянием staging-пометки (PendingDomain,
// фильтр T-108): dom.HTTPPublished = true означает, что предыдущая
// попытка доставки успела опубликовать задачу q:http — публикация
// пропускается (дубль не создаётся), эпизод доходит до записи;
// subs — справочник subdomains (все строки; отключённые пропускает
// CheckSubdomains); askAAAA — dns.ipv6 (ТЗ 5.4: AAAA отключаются
// конфигом без IPv6); met — приборы этапа (T-077; nil — изолированные
// сборки без метрик); log — nil подавляет записи.
func ProcessDomain(
	ctx context.Context,
	pool db.DB,
	q queue.Queue,
	resolve dnsclient.Resolver,
	dom PendingDomain,
	subs []model.Subdomain,
	askAAAA bool,
	met *Metrics,
	log *slog.Logger,
) error {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	if q == nil {
		return fmt.Errorf("dnsworker: домен %s: очередь не задана", dom.Name)
	}
	if resolve == nil {
		return fmt.Errorf("dnsworker: домен %s: резолвер не задан", dom.Name)
	}

	apex, err := CheckApex(ctx, resolve, dom.Name)
	if err != nil {
		return fmt.Errorf("dnsworker: домен %s: апекс: %w", dom.Name, err)
	}

	var (
		rep  *SubdomReport
		wild WildcardResult
	)
	if !apex.Status.Stop() {
		rep, err = CheckSubdomains(ctx, resolve, dom.Name, subs, askAAAA)
		if err != nil {
			return fmt.Errorf("dnsworker: домен %s: субдомены: %w", dom.Name, err)
		}
		info, werr := DetectWildcard(ctx, resolve, dom.Name)
		switch {
		case werr != nil && ctx.Err() != nil:
			// Отмена/дедлайн — эпизод не завершён (не «детект не удался»).
			return fmt.Errorf("dnsworker: домен %s: wildcard-детект: %w", dom.Name, werr)
		case werr != nil:
			// Политика T-038: продолжить без фильтрации — все DNS-живые
			// субдомены идут в HTTP.
			log.Warn("wildcard-детект не удался — продолжаю без фильтрации",
				"domain", dom.Name, "err", werr)
		case info.Detected:
			wild, err = MarkWildcardSubdomains(info.Addrs, resultWildSubs(rep))
			if err != nil {
				return fmt.Errorf("dnsworker: домен %s: wildcard-пометка: %w", dom.Name, err)
			}
		}
	}

	res, err := DecideEpisode(apex, rep, wild)
	if err != nil {
		return fmt.Errorf("dnsworker: домен %s: %w", dom.Name, err)
	}

	// Сначала публикация q:http, затем запись done (порядок — в
	// комментарии файла): сбой записи вернёт эпизод повторной доставкой.
	// Повторная доставка эпизода с уже опубликованной задачей (бит
	// http_published, T-108) публикацию пропускает — доходит до записи.
	switch {
	case !res.HasHTTPTask():
		// Терминальная ветка — публиковать нечего.
	case dom.HTTPPublished:
		log.Debug("q:http уже опубликована предыдущей попыткой — публикация пропущена",
			"domain", dom.Name, "zone_id", dom.ZoneID, "domain_id", dom.DomainID)
	default:
		task := queue.HTTPTask{
			ZoneID:     dom.ZoneID,
			DomainID:   dom.DomainID,
			Name:       dom.Name,
			Subdomains: res.HTTPSubdomains,
		}
		if _, err := q.Publish(ctx, queue.StreamHTTP, task); err != nil {
			return fmt.Errorf("dnsworker: домен %s: публикация q:http: %w", dom.Name, err)
		}
		// Staging-пометка (T-108): failsafe окна «опубликовано, не
		// записано». Non-fatal: отказ пометки не роняет эпизод — запись
		// результата важнее; плата — возможный дубль q:http на повторной
		// доставке (ограничен попытками, ТЗ 3.4), не потеря.
		marked, merr := MarkHTTPPublished(ctx, pool, dom.ZoneID, dom.DomainID)
		switch {
		case merr != nil:
			log.Warn("пометка http_published не записана — повторная доставка может опубликовать дубль q:http",
				"domain", dom.Name, "zone_id", dom.ZoneID, "domain_id", dom.DomainID, "err", merr)
		case !marked:
			log.Debug("пометка http_published не поставлена — домен удалён или уже done",
				"domain", dom.Name, "zone_id", dom.ZoneID, "domain_id", dom.DomainID)
		}
	}

	recorded, err := RecordEpisode(ctx, pool, dom.ZoneID, dom.DomainID, res)
	if err != nil {
		return err
	}
	if !recorded {
		// Гонка с delete-файлом (§13.1) или повторная доставка уже
		// записанного результата — оба случая штатные.
		log.Debug("результат не записан — домен удалён или уже done",
			"domain", dom.Name, "zone_id", dom.ZoneID, "domain_id", dom.DomainID)
		return nil
	}
	if res.SetStatus {
		// Терминальная ветка DNS-этапа — счётчик финальных статусов
		// (T-077, ТЗ 5.9); ветка «есть адреса» статуса не пишет.
		met.DomainStatus(res.Status)
	}
	log.Debug("DNS-эпизод записан",
		"domain", dom.Name, "mask", int64(res.Mask), "http_subs", len(res.HTTPSubdomains),
		"status", res.Status.String(), "set_status", res.SetStatus)
	return nil
}

// resultWildSubs — вход T-038 из отчёта T-037: адреса A/AAAA субдомена
// как net.IP (MarkWildcardSubdomains сравнивает net.IP.Equal — формы
// 4/16 октетов одной точки не различаются).
func resultWildSubs(rep *SubdomReport) []WildSubdomain {
	out := make([]WildSubdomain, 0, len(rep.Results))
	for _, r := range rep.Results {
		addrs := make([]net.IP, 0, len(r.A)+len(r.AAAA))
		for _, a := range r.A {
			addrs = append(addrs, net.IP(a.AsSlice()))
		}
		for _, a := range r.AAAA {
			addrs = append(addrs, net.IP(a.AsSlice()))
		}
		out = append(out, WildSubdomain{ID: r.ID, Label: r.Label, Addrs: addrs})
	}
	return out
}

// LoadSubdomains читает справочник проверяемых субдоменов (ТЗ 5.10):
// источник списка — таблица subdomains (управление crawlerctl, T-083).
// Перечитывается на каждую пачку: справочник крошечный (≤63 строк), а
// изменения (add/disable) подхватываются без рестарта воркеров.
func LoadSubdomains(ctx context.Context, pool db.DB) ([]model.Subdomain, error) {
	if pool == nil {
		return nil, errors.New("dnsworker: справочник субдоменов: пул не задан")
	}
	rows, err := pool.Query(ctx, subdomainsSelectSQL)
	if err != nil {
		return nil, fmt.Errorf("dnsworker: справочник субдоменов: %w", err)
	}
	defer rows.Close()

	subs := make([]model.Subdomain, 0, model.MaxSubdomains)
	for rows.Next() {
		var s model.Subdomain
		if err := rows.Scan(&s.ID, &s.Label, &s.Enabled); err != nil {
			return nil, fmt.Errorf("dnsworker: справочник субдоменов: чтение: %w", err)
		}
		subs = append(subs, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("dnsworker: справочник субдоменов: %w", err)
	}
	if len(subs) == 0 {
		return nil, ErrSubdomainsEmpty
	}
	return subs, nil
}

// resultKey — ключ строки domains в фильтре пачки.
type resultKey struct {
	zone int16
	id   int64
}

// PendingDomain — домен пачки q:dns, ждущий обработки, с состоянием
// staging-пометки публикации (T-108): HTTPPublished = бит http_published
// строки — предыдущая попытка доставки успела опубликовать задачу q:http,
// повтор публикацию пропускает (ProcessDomain) и доходит до записи.
type PendingDomain struct {
	queue.DNSDomain
	HTTPPublished bool
}

// PendingDomains фильтрует пачку q:dns по актуальности строк (один
// SELECT, unnest-пачка — стиль §3.5): остаются домены, чей результат ещё
// не записан (check_state <> done) и которые не удалены (§13.1), вместе с
// битом http_published (T-108) — маркером «задача q:http этого эпизода
// уже опубликована». Повторная доставка пачки (Processing, T-025)
// обрабатывает только их — без дубликатов q:http (опубликованные
// домены перезаписываются без публикации) и без повторной работы по
// записанным; пустой результат — пачка уже отработана, доставка просто
// подтверждается.
func PendingDomains(ctx context.Context, pool db.DB, batch []queue.DNSDomain) ([]PendingDomain, error) {
	if pool == nil {
		return nil, errors.New("dnsworker: фильтр пачки: пул не задан")
	}
	if len(batch) == 0 {
		return nil, nil
	}
	zones := make([]int16, len(batch))
	ids := make([]int64, len(batch))
	byKey := make(map[resultKey]queue.DNSDomain, len(batch))
	for i, d := range batch {
		zones[i] = d.ZoneID
		ids[i] = d.DomainID
		byKey[resultKey{zone: d.ZoneID, id: d.DomainID}] = d
	}

	rows, err := pool.Query(ctx, resultPendingSQL, zones, ids)
	if err != nil {
		return nil, fmt.Errorf("dnsworker: фильтр пачки (%d доменов): %w", len(batch), err)
	}
	defer rows.Close()

	pending := make([]PendingDomain, 0, len(batch))
	for rows.Next() {
		var (
			k     resultKey
			flags int16
		)
		if err := rows.Scan(&k.zone, &k.id, &flags); err != nil {
			return nil, fmt.Errorf("dnsworker: фильтр пачки: чтение: %w", err)
		}
		if d, ok := byKey[k]; ok {
			pending = append(pending, PendingDomain{
				DNSDomain:     d, // ORDER BY u.ord — порядок пачки
				HTTPPublished: model.Flags(flags).IsHTTPPublished(),
			})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("dnsworker: фильтр пачки: %w", err)
	}
	return pending, nil
}

// Дефолты цикла dns-worker.
const (
	// DefaultGroup — группа потребителей q:dns: одна на все экземпляры
	// dns-worker (ТЗ 4.2 — consumer groups масштабируют воркеров
	// горизонтально; записи раздаются внутри группы).
	DefaultGroup = "dns-worker"

	// DefaultConcurrency — предел одновременных эпизодов: зеркалирует
	// dns.concurrency (ТЗ 5.10) — плановый параллелизм DNS-обменов
	// LimitResolver (T-035). Эпизод домена — последовательность обменов,
	// поэтому потолок эпизодов равен потолку обменов; темп отдельного
	// эпизода дополнительно ограничен декоратором резолвера.
	DefaultConcurrency = dnsclient.DefaultLimitConcurrency

	// DefaultConsume — записей q:dns за одно чтение: запись — пачка до
	// scheduler.batch_size доменов (T-028), эпизоды внутри идут параллельно
	// до Concurrency; большее окно чтения не прибавляет работы, пока
	// заняты слоты.
	DefaultConsume = 1

	// DefaultErrorWait — пауза после ошибок БД/Redis: холостые круги не
	// греют ни БД, ни логи (стиль T-028); неподтверждённые доставки
	// вернёт XAUTOCLAIM (T-025) — спешить некуда.
	DefaultErrorWait = 30 * time.Second
)

// Options — параметры цикла dns-worker; нулевые значения заменяются
// дефолтами (withDefaults, стиль scheduler.EnqueueOptions).
type Options struct {
	// Concurrency — максимум одновременных DNS-эпизодов (доменов);
	// 0 — DefaultConcurrency (dns.concurrency, ТЗ 5.10).
	Concurrency int
	// Group — группа потребителей q:dns; "" — DefaultGroup.
	Group string
	// Consumer — имя потребителя группы; "" — "dns-worker-<pid>"
	// (уникально в пределах экземпляра — различает PEL владельцев).
	Consumer string
	// Consume — записей q:dns за одно чтение; 0 — DefaultConsume.
	Consume int
	// AskAAAA — запрашивать ли AAAA субдоменов (dns.ipv6, ТЗ 5.4).
	AskAAAA bool
	// Recover — слой возврата зависших доставок (Reliab, T-025): Run
	// поднимает фоновый RecoverLoop (XAUTOCLAIM → та же обработка →
	// XACK) с теми же Group/Consumer; nil — восстановления нет
	// (изолированные сборки и тесты без Redis-специфики).
	Recover *queue.Reliab
	// RecoverBatch — пачка клейма RecoverLoop за проход; 0 — дефолт
	// очереди (queue.DefaultRecoverBatch). Боевая сборка передаёт
	// redis.recover_batch (T-153): темп возврата зависших — настраиваемая
	// величина очереди, а не окно потребления этапа.
	RecoverBatch int
	// RecoverInterval — период опроса зависших записей RecoverLoop;
	// 0 — дефолт очереди (queue.DefaultRecoverInterval). Конфиг —
	// redis.recover_interval (T-153).
	RecoverInterval time.Duration
	// Metrics — реестр Prometheus сервиса (T-077, ТЗ 5.9): из него Run
	// строит приборы этапа (NewMetrics — счётчик эпизодов, гистограмма
	// длительностей, счётчик финальных статусов); nil — без метрик
	// (изолированные сборки и тесты).
	Metrics *metrics.Registry
	// Log — логгер цикла; nil — записи подавлены (slog.DiscardHandler).
	Log *slog.Logger
}

func (o Options) withDefaults() Options {
	if o.Concurrency <= 0 {
		o.Concurrency = DefaultConcurrency
	}
	if o.Group == "" {
		o.Group = DefaultGroup
	}
	if o.Consumer == "" {
		o.Consumer = fmt.Sprintf("dns-worker-%d", os.Getpid())
	}
	if o.Consume <= 0 {
		o.Consume = DefaultConsume
	}
	return o
}

// Run — рабочий цикл dns-worker (ТЗ 5.4): читает пачки q:dns группой
// потребителей и обрабатывает каждую доставку паттерном Processing
// (T-025): эпизоды доменов → публикация q:http → запись результата →
// XACK. Работает до отмены ctx и возвращает nil — контракт OnRun каркаса
// (T-012: graceful shutdown — начатые эпизоды дорабатывают или
// возвращаются очереди повторной доставкой). Ошибка чтения или обработки
// не останавливает цикл: логируется (Warn), неподтверждённая доставка
// вернётся XAUTOCLAIM (ТЗ 4.2).
func Run(ctx context.Context, pool db.DB, q queue.Queue, resolve dnsclient.Resolver, opts Options) error {
	if pool == nil {
		return errors.New("dnsworker: цикл воркера: пул не задан")
	}
	if q == nil {
		return errors.New("dnsworker: цикл воркера: очередь не задана")
	}
	if resolve == nil {
		return errors.New("dnsworker: цикл воркера: резолвер не задан")
	}
	o := opts.withDefaults()
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	met := NewMetrics(o.Metrics) // nil без реестра — приборы no-op (T-077)
	log.Info("цикл dns-worker",
		"group", o.Group, "consumer", o.Consumer,
		"concurrency", o.Concurrency, "consume", o.Consume, "ask_aaaa", o.AskAAAA)

	wait := func(d time.Duration) {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-ctx.Done():
		case <-t.C:
		}
	}

	// Возврат зависших доставок (T-025): фоновый RecoverLoop от имени
	// того же потребителя — клейм XAUTOCLAIM и та же обработка пачек.
	// Живёт до отмены ctx; Run дожидается его выхода (начатая пачка
	// дорабатывает или остаётся неподтверждённой, ТЗ 3.4).
	if o.Recover != nil {
		recDone := make(chan struct{})
		go func() {
			defer close(recDone)
			_ = queue.RecoverLoop(ctx, o.Recover, func(c context.Context, del queue.Delivery) error {
				return processBatch(c, pool, q, resolve, del, o, met, log)
			}, queue.RecoverOptions{
				Stream:   queue.StreamDNS,
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

	for {
		deliveries, err := q.Consume(ctx, queue.StreamDNS, o.Group, o.Consumer, o.Consume)
		if err != nil {
			if ctx.Err() != nil {
				return nil // graceful shutdown
			}
			log.Warn("цикл dns-worker: чтение q:dns", "err", err)
			wait(DefaultErrorWait)
			continue
		}
		for _, d := range deliveries {
			if ctx.Err() != nil {
				return nil // остановка: доставка не читалась — не подтверждена
			}
			err := queue.Processing(ctx, q, o.Group, d, func(c context.Context, del queue.Delivery) error {
				return processBatch(c, pool, q, resolve, del, o, met, log)
			})
			if err == nil {
				continue
			}
			if ctx.Err() != nil {
				return nil // отмена в работе — повторная доставка, не тревога
			}
			log.Warn("пачка q:dns не подтверждена — вернётся повторной доставкой",
				"entry", d.ID, "err", err)
		}
	}
}

// processBatch обрабатывает одну доставку q:dns (пачка доменов, T-024):
// справочник субдоменов → фильтр актуальности (PendingDomains) → эпизоды
// доменов параллельно до Concurrency (dns.concurrency, ТЗ 5.10). Ошибка
// любого эпизода — ошибка всей доставки (без XACK): повторная доставка
// прогонит пачку заново, фильтр отсечёт уже записанные домены, а бит
// http_published (T-108) исключит дубликаты публикации — повторяется
// только недоработанное (идемпотентность, ТЗ 3.4).
// Завершённые эпизоды идут в приборы met (T-077): счётчик задач этапа,
// гистограмма длительностей (статусы терминальных веток считает
// ProcessDomain после записи).
//
// Не ронывают доставку детерминированные состояния, которые повторная
// доставка не лечит (DNS-03): пустой или целиком отключённый/кривой
// справочник субдоменов — конфигурация, а не яд (пачка подтверждается с
// Warn, домены вернёт lease-проход T-029/T-030 после починки справочника
// — повторами до q:dead очередь замусоривалась бы каждой пачкой, а
// терминальный статус заморозил бы домены навсегда: они исправны и
// обязаны обработаться после починки); домен с пустым именем — мусорная
// строка (T-109): фиксируется терминальный диагноз invalid
// (RecordInvalid), Warn по домену и продолжение — остальные домены пачки
// обрабатываются, доставка подтверждается; строка выводится из цикла
// переиздания (recheck invalid не возвращает). Транспортные сбои
// (БД/Redis) и порченое сообщение (Decode) — по-прежнему ошибка без
// XACK: их повторная доставка лечит (T-025).
func processBatch(
	ctx context.Context,
	pool db.DB,
	q queue.Queue,
	resolve dnsclient.Resolver,
	del queue.Delivery,
	o Options,
	met *Metrics,
	log *slog.Logger,
) error {
	batch, err := queue.Decode[queue.DNSBatch](del)
	if err != nil {
		// Ядовитая запись: повторная доставка бессмысленна, но очередь
		// попытается — исчерпает попытки и уйдёт в q:dead (T-025).
		return fmt.Errorf("dnsworker: запись %s стрима %s: %w", del.ID, del.Stream, err)
	}
	subs, err := LoadSubdomains(ctx, pool)
	if err != nil {
		if errors.Is(err, ErrSubdomainsEmpty) {
			log.Warn("справочник subdomains пуст — пачка q:dns подтверждена без обработки",
				"entry", del.ID, "batch", len(batch.Domains), "err", err)
			return nil
		}
		return err
	}
	enabled, err := filterEnabledSubdomains(subs)
	if err != nil || len(enabled) == 0 {
		// Кривые строки или все субдомены отключены (штатная операция
		// crawlerctl subdomains disable) — конфигурация: повтор доставки
		// не изменит исход.
		if err != nil {
			log.Warn("справочник subdomains некорректен — пачка q:dns подтверждена без обработки",
				"entry", del.ID, "batch", len(batch.Domains), "err", err)
		} else {
			log.Warn("нет включённых субдоменов — пачка q:dns подтверждена без обработки",
				"entry", del.ID, "batch", len(batch.Domains))
		}
		return nil
	}
	pending, err := PendingDomains(ctx, pool, batch.Domains)
	if err != nil {
		return err
	}
	if len(pending) == 0 {
		log.Debug("пачка q:dns уже обработана или исключена (done/deleted)",
			"entry", del.ID, "batch", len(batch.Domains))
		return nil
	}

	// Семафор ограничивает одновременные эпизоды (глубина — Concurrency,
	// приобретается до запуска горутины — очередь не растёт); ошибки
	// собираются до первой (mutex, без errgroup — зависимость косвенная).
	sem := make(chan struct{}, o.Concurrency)
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)
	for i := range pending {
		if err := ctx.Err(); err != nil {
			// Остановка: не начатые домены вернутся повторной доставкой.
			mu.Lock()
			if firstErr == nil {
				firstErr = err
			}
			mu.Unlock()
			break
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(dom PendingDomain) {
			defer wg.Done()
			defer func() { <-sem }()
			start := time.Now()
			err := ProcessDomain(ctx, pool, q, resolve, dom, subs, o.AskAAAA, met, log)
			switch {
			case err == nil:
				// Эпизод завершён — пропускная способность и
				// длительность этапа (T-077, ТЗ 5.9).
				met.Episode(time.Since(start))
			case errors.Is(err, ErrEmptyDomain):
				// Ядовитый домен: детерминированная ошибка, повтор
				// доставки не изменит исход — терминальный диагноз invalid
				// (T-109) вместо вечного цикла queued→lease→dispatch.
				// Warn и продолжение: остальные домены пачки обрабатываются,
				// доставка подтверждается. Сбой записи диагноза — ошибка
				// эпизода (без XACK): повторная доставка доиграет фиксацию.
				recorded, rerr := RecordInvalid(ctx, pool, dom.ZoneID, dom.DomainID)
				switch {
				case rerr != nil:
					mu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("домен %s: %w", dom.Name, rerr)
					}
					mu.Unlock()
				case recorded:
					met.DomainStatus(model.StatusInvalid)
					log.Warn("домен с пустым именем — зафиксирован диагноз invalid, строка выведена из цикла переиздания",
						"entry", del.ID, "zone_id", dom.ZoneID, "domain_id", dom.DomainID)
				default:
					// Охраны отсекли (deleted/уже done) — повторная доставка
					// или конкурентный delete: штатно.
					log.Debug("диагноз invalid не записан — домен удалён или уже done",
						"entry", del.ID, "zone_id", dom.ZoneID, "domain_id", dom.DomainID)
				}
			default:
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("домен %s: %w", dom.Name, err)
				}
				mu.Unlock()
			}
		}(pending[i])
	}
	wg.Wait()

	if firstErr != nil {
		return fmt.Errorf("dnsworker: пачка %s: %d/%d доменов: %w", del.ID, len(pending), len(batch.Domains), firstErr)
	}
	log.Debug("пачка q:dns обработана", "entry", del.ID,
		"batch", len(batch.Domains), "pending", len(pending))
	return nil
}

// Retention контента и трим jsonb не-находок (T-166; ТЗ 5.7, 5.5, 6.1, 3.2).
//
// Диск без новых мощностей (решение оператора 2026-10-23): расширение
// тома недоступно, S3 и архивирование отложены, рост ≈36 GiB/сут
// (контент 20.3 + БД 15.5, замер docs/load-measurements.md §12)
// останавливается окном времени вместо мощностей. Контент и тяжёлые
// jsonb нужны конвейеру ограниченное время: анализатор читает тела и
// headers/cookies (analyzeSelectSQL, ТЗ 5.8), рендер заменяет контент
// SPA-записей (ТЗ 5.6), переанализ находок (T-074) переприменяет
// правила к сохранённым страницам. Запись, остаревшая сверх
// content.retention и не нужная более этим потребителям, теряет ссылку
// на контент и jsonb; паспорт строки (http_status, final_url, error,
// response_ms, checked_at, scheme, subdomain_id, variant) остаётся
// всегда — domain_info (ТЗ 5.11) и дашборд продолжают отвечать.
//
// Критерий отбора (все части обязательны):
//
//   - checked_at старше content.retention: время пишет now() сервера БД
//     (T-058), им же считается граница старения — один источник;
//   - домен НЕ Laravel-находка: нет строки laravel_domains (находка —
//     уверенные срабатывания, порог §13.1, aggregate.go). Находки
//     хранят контент всегда: доказательство и материал переанализа
//     T-074; слабые срабатывания (laravel_findings с is_laravel=false)
//     находкой не считаются — отступление зафиксировано в задаче;
//   - запись не ждёт рендера: домен без флага render_pending либо
//     запись уже rendered. Проход доработки T-066 перечитывает контент
//     для повторного SPA-вердикта, rendered=true — рендер уже заменил
//     контент (T-067), старый не нужен;
//   - домен не удалён (§13.1: deleted — территория фоновой очистки
//     T-020).
//
// Одним UPDATE запись теряет и ссылку, и jsonb: content_hash → NULL,
// headers/cookies/tls/redirects/addrs/cname → NULL. Колонки
// неиндексированные (индексов на subdomain_results, кроме PK, нет) —
// UPDATE остаётся HOT (fillfactor 85, миграция 000002), место страниц
// возвращает автовакуум. Срезание ссылки и уменьшение contents.refcount
// — ОДНА транзакция (ТЗ 3.4): сбой между ними оставлял бы контент без
// владельца или refcount навсегда завышенным (инварианты T-044). Строка
// contents, чей refcount дошёл до нуля, удаляется сразу, а не только
// помечается мёртвой (как у очистки T-020): retention — массовый
// источник мёртвых блоков, и строки contents иначе копились бы до
// компактификации затронутых паков (пак с долей живых ≥ порога её не
// запускает). Физическое место блоков в паках возвращает компактификация
// T-045: purge лишь прекращает право контента на место.
//
// Интерлок q:analyze: анализатор — потребитель тел и headers/cookies;
// глубина стрима (backlog группы потребления, T-026) у порога означает
// «не успевает» — записи очереди ещё не разобраны. Проход и каждая
// пачка дренажа пропускаются, пока глубина ≥ порога: тела ещё нужны.
// Ошибка чтения глубины — тоже пропуск (консервативно, как HasCapacity
// у T-066): чистить «наугад» дороже, чем отложить проход. Порог 0 —
// интерлок выключен.
//
// Точная защита по непотреблённым сообщениям (retentionpending.go): флаг
// render_pending не покрывает записи, чьи сообщения УЖЕ опубликованы в
// q:render/q:analyze (публикация состоялась — флага нет), а интерлок не
// видит застарелых сообщений при глубине ниже порога; перед проходом
// ключи непотреблённых сообщений вычитываются из стримов, и записи,
// ожидающие потребителей, исключаются из пачек дренажа.
//
// Пачки и дренаж: выборка кандидатов — курсорная пагинация по времени
// (checked_at, PK) > курсор (T-185): границы checked_at обслуживает
// индекс subdomain_results_checked_idx (T-171), каждая пачка — короткая
// транзакция (стиль T-020). Курсор цикла переживает проходы: строка
// посещается один раз за жизнь (очищенная остаётся позади курсора,
// перепроверённая DELETE+INSERT с fresh checked_at входит в дренаж
// заново, когда состарится), цена прохода ∝ притоку, а не размеру
// таблицы. Взамен упразднённых полных кругов — явные ограниченные
// ретраи (retentiondrain.go): bounded-список ключей защиты очередей с
// точечными предложениями и catch-up-круг по обнулению счётчика
// рендер-долга. Повторный проход безопасен: очищенные записи выпадают
// из отбора (content_hash и jsonb уже NULL), охраны UPDATE отсеивают
// гонки с перезаписью (recheck T-030 пишет fresh checked_at и новый
// контент — двойная проверка checked_at и content_hash IS NOT
// DISTINCT FROM).
//
// Зафиксированные отступления (согласованы с заказчиком, T-166):
// переанализ не-находок новыми правилами невозможен (применение — на
// recheck ≈6 мес); перепроверка теряет сравнение «изменился ли сайт»
// (перезапись DELETE+INSERT пишет результат начисто, T-058).
//
// Живёт цикл в http-worker (cmd/http-worker): сервис уже держит пул БД,
// гейт глубин стримов (интерлок) и реестр метрик, рядом с компактификацией
// T-045, которую purge и «кормит»; второму владельцу цикл не нужен
// (охраны делают параллельный запуск безопасным, но бесполезным — как
// у компактификации).
package content

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/db"
	"crawler/internal/model"
	"crawler/internal/queue"
)

// Дефолты цикла (ТЗ 13.2: значения по умолчанию уточняются замерами;
// рабочие значения передаёт конфигурация, константы — единый источник
// для сбо́ки сервиса и тестов).
const (
	// DefaultRetention — окно жизни контента не-находок: 6 ч. Замер §12:
	// стационарный объём контента = суточный приток × окно — при
	// 20 GiB/сут окно 48 ч держало бы ≈40 GiB и съедало бы том, 6 ч ≈
	// 5 GiB; окно перекрывает латентности потребителей (анализ —
	// секунды на запись) с запасом на сбои и лаги очередей.
	DefaultRetention = 6 * time.Hour

	// DefaultRetentionInterval — период проходов RetentionLoop: час, как
	// компактификация паков (T-045) — место возвращает её проход,
	// retention лишь готовит мёртвые блоки; чаще — лишние сканы
	// старения, реже — окно плавает.
	DefaultRetentionInterval = time.Hour

	// DefaultRetentionBatch — строк в одной пачке-транзакции. Вилка
	// постановки 10–50 тыс.; нижняя граница дренирует приток ≈3 млн
	// строк/сут (замер §12) за ~13 пачек часового прохода, транзакция
	// держит блокировки строк пачки минимальное время.
	DefaultRetentionBatch = 10000
)

// SQL retention. Коды схемы (бит render_pending, статус deleted) —
// литералами, а не данными (стиль record.go T-058), потому var, как
// SQL флага renderq.go; тексты закреплены тестами (retention_test.go).
// Граница старения — now() − interval на сервере: checked_at пишет тот
// же now() транзакции записи (T-058), один источник времени; в
// транзакции now() стабилен (момент Begin).
var (
	// retentionSelectSQL — пачка кандидатов: есть что чистить (живой
	// content_hash или jsonb), запись остарела, не ждёт рендера, домен
	// жив и не находка. Курсор row-value > по (checked_at, PK) —
	// пагинация дренажа по времени (T-185, была по PK): обе границы
	// checked_at — условия индекса subdomain_results_checked_idx (T-171),
	// нижнюю планировщик PG16 выводит из row-value сам (проверено
	// EXPLAIN: Index Cond checked_at >= $2 AND checked_at < now()-$1) —
	// скан обязан читать только окно старения впереди курсора, а не
	// heap всей истории (диагноз запроса №13 журнала медленных 24.10);
	// ORDER BY по тому же ключу — детерминированные пачки: узкое окно
	// устойчивого дренажа собирает Incremental Sort поверх порядка
	// индекса, широкий (catch-up-круг с нуля) — Sort среза (замер
	// EXPLAIN 24.10, PG 16.15). checked_at в списке колонок — курсор
	// следующей пачки.
	retentionSelectSQL = "SELECT r.zone_id, r.domain_id, r.subdomain_id, r.variant, r.content_hash, r.checked_at " +
		"FROM subdomain_results AS r " +
		"JOIN domains AS d ON d.zone_id = r.zone_id AND d.id = r.domain_id " +
		"WHERE r.checked_at < now() - $1::interval " +
		"AND (r.checked_at, r.zone_id, r.domain_id, r.subdomain_id, r.variant) " +
		"> ($2::timestamptz, $3::int2, $4::int8, $5::int2, $6::int2) " +
		"AND (r.content_hash IS NOT NULL OR r.headers IS NOT NULL OR r.cookies IS NOT NULL " +
		"OR r.tls IS NOT NULL OR r.redirects IS NOT NULL OR r.addrs IS NOT NULL OR r.cname IS NOT NULL) " +
		fmt.Sprintf("AND (r.rendered OR (d.flags & %d) = 0) ", int16(model.FlagRenderPending)) +
		fmt.Sprintf("AND d.status <> %d ", int16(model.StatusDeleted)) +
		"AND NOT EXISTS (SELECT 1 FROM laravel_domains AS ld " +
		"WHERE ld.zone_id = r.zone_id AND ld.domain_id = r.domain_id) " +
		"ORDER BY r.checked_at, r.zone_id, r.domain_id, r.subdomain_id, r.variant " +
		"LIMIT $7"

	// retentionProbeSQL — точечное предложение ретраев защиты очередей
	// (T-185): ключи, отложенные снимком q:render/q:analyze, лежат позади
	// курсора и не вернутся полным кругом (круги упразднены) — следующий
	// проход предлагает их зондом по PK с фильтрами отбора выборки, но
	// без курсора. Точечность плана закреплена JOIN LATERAL LIMIT 1
	// (ревью T-185, P2-1): простой unnest-join при длинном списке (застой
	// очередей — а список тогда длинный, лимит 100 тыс.) планировщик
	// перевыбирает в hash join + seq scan по всей остаревшей истории —
	// дорожка, убираемая задачей; LATERAL держит Index Scan по PK на
	// каждый ключ при любом объёме списка (проверено EXPLAIN на PG16),
	// стиль закрепления формы плана — как SET LOCAL у выборки T-181.
	// Ключи, переставшие быть кандидатами (защита ушла и очистка прошла
	// иным механизмом, перезапись recheck с fresh checked_at, находка,
	// рендер-ожидание — его догонит catch-up-круг по счётчику долга),
	// зонд не возвращает — ключ выпадает из списка (retentiondrain.go).
	retentionProbeSQL = "SELECT k.zone_id, k.domain_id, k.subdomain_id, k.variant, r.content_hash " +
		"FROM unnest($1::int2[], $2::int8[], $3::int2[], $4::int2[]) " +
		"AS k(zone_id, domain_id, subdomain_id, variant) " +
		"JOIN LATERAL (" +
		"SELECT r.zone_id, r.domain_id, r.subdomain_id, r.variant, r.content_hash " +
		"FROM subdomain_results AS r " +
		"JOIN domains AS d ON d.zone_id = r.zone_id AND d.id = r.domain_id " +
		"WHERE r.zone_id = k.zone_id AND r.domain_id = k.domain_id " +
		"AND r.subdomain_id = k.subdomain_id AND r.variant = k.variant " +
		"AND r.checked_at < now() - $5::interval " +
		"AND (r.content_hash IS NOT NULL OR r.headers IS NOT NULL OR r.cookies IS NOT NULL " +
		"OR r.tls IS NOT NULL OR r.redirects IS NOT NULL OR r.addrs IS NOT NULL OR r.cname IS NOT NULL) " +
		fmt.Sprintf("AND (r.rendered OR (d.flags & %d) = 0) ", int16(model.FlagRenderPending)) +
		fmt.Sprintf("AND d.status <> %d ", int16(model.StatusDeleted)) +
		"AND NOT EXISTS (SELECT 1 FROM laravel_domains AS ld " +
		"WHERE ld.zone_id = r.zone_id AND ld.domain_id = r.domain_id) " +
		"LIMIT 1) AS r ON true"

	// retentionDomainSQL — точечный дренаж контента одного домена (T-187,
	// retentiondomain.go): снятие Laravel-находки переанализом
	// (aggregateDomain, не-fresh) возвращает остаревшие записи домена в
	// множество кандидатов ПОЗАДИ курсора дренажа (строка посещается один
	// раз за жизнь, T-185) — часовой проход их больше не увидит. Фильтры —
	// те же, что у retentionSelectSQL, но адресация по префиксу PK
	// (zone_id, domain_id) и без курсора: лист партиции/субпартиции
	// (T-005) режет выборку до строк домена, курсор дренажа не трогается
	// (очищенные строки выпадут из любого будущего круга сами). Строк у
	// домена ограниченное число (субдомены 0..62 × варианты, §5.10) —
	// LIMIT не нужен; ORDER BY PK — детерминированные аргументы пачки
	// (стиль cleanupRefcounts T-020) и порядок блокировок строк.
	retentionDomainSQL = "SELECT r.zone_id, r.domain_id, r.subdomain_id, r.variant, r.content_hash, r.checked_at " +
		"FROM subdomain_results AS r " +
		"JOIN domains AS d ON d.zone_id = r.zone_id AND d.id = r.domain_id " +
		"WHERE r.zone_id = $1::int2 AND r.domain_id = $2::int8 " +
		"AND r.checked_at < now() - $3::interval " +
		"AND (r.content_hash IS NOT NULL OR r.headers IS NOT NULL OR r.cookies IS NOT NULL " +
		"OR r.tls IS NOT NULL OR r.redirects IS NOT NULL OR r.addrs IS NOT NULL OR r.cname IS NOT NULL) " +
		fmt.Sprintf("AND (r.rendered OR (d.flags & %d) = 0) ", int16(model.FlagRenderPending)) +
		fmt.Sprintf("AND d.status <> %d ", int16(model.StatusDeleted)) +
		"AND NOT EXISTS (SELECT 1 FROM laravel_domains AS ld " +
		"WHERE ld.zone_id = r.zone_id AND ld.domain_id = r.domain_id) " +
		"ORDER BY r.zone_id, r.domain_id, r.subdomain_id, r.variant"

	// retentionDebtSQL — точечное чтение счётчика рендер-долга
	// crawler_counters['render_pending'] (T-175) в начале каждого
	// прохода (T-185): переход счётчика в 0 — момент снятия всей
	// рендер-защиты разом, цикл планирует catch-up-круг с нулевого
	// курсора. Текст — тот же точечный SELECT, что guard прохода
	// доработки renderDebtCounterSQL (T-181, internal/httpworker/
	// renderq.go): хелпер не экспортирован, литерал дублируется с
	// якорем (стиль pendingFieldData в cmd/http-worker/main.go);
	// контракт счётчика (миграция 000015, переходы бита ведут пару ±1)
	// не меняется — retention только читает.
	retentionDebtSQL = "SELECT value FROM crawler_counters WHERE name = 'render_pending'"

	// retentionPurgeSQL — чистка пачки: ссылка на контент и jsonb → NULL
	// одной командой (одна версия строки — один HOT-апдейт вместо двух).
	// Двойная охрана от гонки с перезаписью результата (recheck T-030
	// переписывает строки DELETE+INSERT со fresh checked_at и, возможно,
	// тем же content_hash): повторная проверка возраста и
	// content_hash IS NOT DISTINCT FROM прочитанного (NULL корректен —
	// «контента нет» легален, T-056; сравнение как в renderUpdateSQL
	// T-067). RETURNING k.hash — хэш из unnest-входа (новое значение
	// строки — всегда NULL): по не-NULL возвращённым считается, сколько
	// ссылок срезано и на сколько уменьшать refcount. Паспорт строки
	// (http_status, final_url, error, response_ms, checked_at, scheme,
	// rendered, rules_version) не трогается.
	retentionPurgeSQL = "UPDATE subdomain_results AS r " +
		"SET content_hash = NULL, headers = NULL, cookies = NULL, " +
		"tls = NULL, redirects = NULL, addrs = NULL, cname = NULL " +
		"FROM unnest($1::int2[], $2::int8[], $3::int2[], $4::int2[], $5::bytea[]) " +
		"AS k(zone_id, domain_id, subdomain_id, variant, hash) " +
		"WHERE r.zone_id = k.zone_id AND r.domain_id = k.domain_id " +
		"AND r.subdomain_id = k.subdomain_id AND r.variant = k.variant " +
		"AND r.checked_at < now() - $6::interval " +
		"AND r.content_hash IS NOT DISTINCT FROM k.hash " +
		"RETURNING k.hash"

	// retentionDecrefSQL — уменьшение refcount по фактически срезанным
	// ссылкам (агрегированным по хэшу: один контент может ссылаться из
	// многих записей — дедупликация, ТЗ 5.7). GREATEST держит счётчик
	// неотрицательным при рассинхронизации (стиль cleanupUpdateSQL
	// T-020: минус сломал бы инвариант «0 = мёртвый»); RETURNING отдаёт
	// новое значение — строки, дошедшие до нуля, удаляются следующим
	// оператором, len нужен для счётчика освобождённых байтов.
	retentionDecrefSQL = "UPDATE contents AS c " +
		"SET refcount = GREATEST(c.refcount - h.n, 0) " +
		"FROM unnest($1::bytea[], $2::int4[]) AS h(hash, n) " +
		"WHERE c.hash = h.hash " +
		"RETURNING c.hash, c.refcount, c.len"

	// retentionDeleteSQL — удаление строк contents, лишившихся последней
	// ссылки. Охрана refcount <= 0 (стиль sqlCompactDead T-045):
	// между decref и delete одной транзакции воскрешения не бывает,
	// охрана — страховка инварианта «0 = мёртвый».
	retentionDeleteSQL = "DELETE FROM contents " +
		"WHERE hash = ANY($1::bytea[]) AND refcount <= 0"
)

// RetentionDB — пул для retention: db.DB, из которого можно начать
// транзакцию (стиль ingest.CleanupDB). Срезание ссылок и уменьшение
// refcount атомарны в одной транзакции (ТЗ 3.4); *pgxpool.Pool
// удовлетворяет интерфейсу.
type RetentionDB interface {
	db.DB
	Begin(ctx context.Context) (pgx.Tx, error)
}

// compile-time: реальный пул подходит под интерфейс.
var _ RetentionDB = (*pgxpool.Pool)(nil)

// RetentionDepth — источник глубины q:analyze для интерлока: глубина
// (backlog группы потребления — min(lag, XLEN) + pending, T-026/T-168)
// у порога значит, что анализатор не успевает и записи очереди ещё
// не разобраны.
// Узкий интерфейс — пакет не зависит от Redis-специфики гейта
// (стиль RenderLen T-066); сборка сервиса подключает адаптер над
// BackpressureGate.
type RetentionDepth interface {
	AnalyzeDepth(ctx context.Context) (int64, error)
}

// RetentionOptions — параметры цикла; нулевые значения заменяются
// дефолтами (withDefaults, стиль CleanupOptions T-020).
type RetentionOptions struct {
	// Retention — окно жизни контента не-находок (DefaultRetention);
	// 0 — retention выключен (RetentionLoop завершается сразу).
	Retention time.Duration
	// Interval — период проходов цикла (DefaultRetentionInterval); на
	// разовый RetentionOnce не влияет.
	Interval time.Duration
	// BatchSize — максимум строк в одной пачке-транзакции
	// (DefaultRetentionBatch).
	BatchSize int
	// Depth — источник глубины q:analyze (интерлок); nil — интерлок
	// выключен (проход не ждёт анализатора — осознанный режим,
	// управляемый AnalyzeWatermark = 0 в сборке сервиса).
	Depth RetentionDepth
	// AnalyzeWatermark — порог интерлока: глубина q:analyze ≥ порога —
	// пропуск прохода/пачки; ≤ 0 — интерлок выключен.
	AnalyzeWatermark int64
	// Pending — источник непотреблённых ключей q:render/q:analyze (точная
	// защита записей, ожидающих очередей, — retentionpending.go); nil —
	// защита выключена (изолированные сборки, юнит-тесты критериев
	// отбора). В отличие от интерлока порога не имеет: исключаются ровно
	// те записи, на которые указывают живые сообщения стримов.
	Pending RetentionPending
	// Log — логгер цикла; nil — записи подавлены (slog.DiscardHandler).
	Log *slog.Logger
	// OnPass — хук итога успешного прохода: снапшот RetentionStats и
	// длительность сразу после дренажа (ошибочный проход хуку не
	// отдаётся — его записал Warn). Точка подключения метрик без
	// зависимости цикла от internal/metrics (стиль CleanupOptions.OnPass
	// T-020/T-099). Обязан быть быстрым и не паниковать.
	OnPass func(RetentionStats, time.Duration)
}

// withDefaults заменяет некорректные нули дефолтами; Retention не
// трогает: 0 — легальное «выключено».
func (o RetentionOptions) withDefaults() RetentionOptions {
	if o.Interval <= 0 {
		o.Interval = DefaultRetentionInterval
	}
	if o.BatchSize <= 0 {
		o.BatchSize = DefaultRetentionBatch
	}
	return o
}

// RetentionStats — итог одного прохода RetentionOnce.
type RetentionStats struct {
	// Rows — строки subdomain_results, очищенные проходом (срезаны
	// content_hash и/или jsonb).
	Rows int64
	// Refs — срезанные ссылки content_hash (не-NULL хэши очищенных
	// строк, включая некорректные по длине).
	Refs int64
	// Contents — удалённые строки contents (refcount дошёл до нуля).
	Contents int64
	// FreedBytes — место освобождённых блоков (сжатые len удалённых
	// contents): физически его вернёт компактификация паков (T-045).
	FreedBytes int64
	// Batches — пачек-транзакций выполнено.
	Batches int
	// BadHashes — срезанные ссылки с content_hash неверной длины
	// (контракт — 16 октет, ТЗ 6.1): refcount по ним не менялся;
	// в норме не растёт вовсе — рост = ошибка писателя (T-056),
	// счётчик-аналог CleanupResult.BadHashes (T-020/T-099).
	BadHashes int64
	// Missing — срезанные ссылки без строки contents (расхождение
	// учёта): refcount по ним не менялся, аномалия для алерта.
	Missing int64
	// Skipped — проход остановлен интерлоком: глубина q:analyze у порога
	// (анализатор не успевает, тела ещё нужны); данные не тронуты.
	Skipped bool
	// SkippedPending — проход остановлен ошибкой чтения непотреблённых
	// ключей q:render/q:analyze (снимок до прохода или обновление при
	// длинном дренаже): консервативный пропуск — записи очередей не
	// тронуты, пачки до ошибки зафиксированы (пачки атомарны сами по
	// себе); итог уходит в OnPass — Warn в цикле и метрика
	// crawler_content_retention_pending_errors_total.
	SkippedPending bool
	// Depth — глубина q:analyze в момент остановки (для лога).
	Depth int64
	// Deferred — записи-кандидаты, исключённые из чистки прохода: их
	// сообщения ждут потребителей стримов (ключ — стрим: q:render либо
	// q:analyze; запись публикуется в оба — считается в каждом, кто
	// держит). Растёт при застое очередей — материал алерта рядом со
	// skips_total (retentionmetrics.go: crawler_content_retention_
	// deferred_total{stream}).
	Deferred map[string]int64
}

// add суммирует итог пачки в накопитель прохода.
func (s *RetentionStats) add(o RetentionStats) {
	s.Rows += o.Rows
	s.Refs += o.Refs
	s.Contents += o.Contents
	s.FreedBytes += o.FreedBytes
	s.Batches += o.Batches
	s.BadHashes += o.BadHashes
	s.Missing += o.Missing
	if len(o.Deferred) > 0 {
		if s.Deferred == nil {
			s.Deferred = make(map[string]int64, len(o.Deferred))
		}
		for k, v := range o.Deferred {
			s.Deferred[k] += v
		}
	}
}

// retentionKey — ключ строки subdomain_results (PK, ТЗ 6.1): хвост
// курсора пагинации дренажа (детерминизм внутри одинаковых checked_at) и
// единица списка ретраев защиты очередей (T-185).
type retentionKey struct {
	zone    int16
	domain  int64
	sub     int16
	variant int16
}

// less — лексикографический порядок ключа (row-value, ТЗ 6.1):
// детерминизм пачек и сортировка ключей ретраев для воспроизводимых
// зондов.
func (k retentionKey) less(b retentionKey) bool {
	if k.zone != b.zone {
		return k.zone < b.zone
	}
	if k.domain != b.domain {
		return k.domain < b.domain
	}
	if k.sub != b.sub {
		return k.sub < b.sub
	}
	return k.variant < b.variant
}

// retentionCursor — курсор дренажа по времени (T-185): последняя
// посещённая строка, переживающая проходы цикла (retentiondrain.go).
// Время ведёт — строки стареют в порядке checked_at; PK детерминирует
// порядок внутри одинаковых отметок (now() стабильна в транзакции
// записи — пачки строк делят отметку, T-058). Нулевое значение — «с
// начала» (catch-up-круг: старт процесса, снятие рендер-долга,
// переполнение списка ретраев, fallback-сутки); time.Time{} меньше
// любого реального checked_at.
type retentionCursor struct {
	checkedAt time.Time
	key       retentionKey
}

// retentionCandidate — строка пачки: ключ, прочитанный content_hash
// (nil — у записи только jsonb, ссылки нет) и отметка checked_at —
// курсор следующей пачки (T-185).
type retentionCandidate struct {
	key       retentionKey
	hash      []byte
	checkedAt time.Time
}

// RetentionOnce — один проход с нулевого курсора (catch-up-круг):
// дренаж старения пачками по BatchSize до исчерпания множества,
// остановки интерлоком, ошибки защиты очередей или отмены ctx.
// Идемпотентен (ТЗ 3.4): очищенные строки выпадают из отбора, пачки
// атомарны. Записи, чьи сообщения ждут q:render/q:analyze,
// исключаются из пачек по снимку непотреблённых ключей (retentionpending.go):
// снимок берётся до прохода и обновляется каждые
// DefaultRetentionPendingRefresh выборок длинного дренажа; ошибка чтения
// стримов — консервативный пропуск остатка прохода (SkippedPending).
// Разовый проход без состояния цикла (курсор/ретраи живут в
// retentionState, retentiondrain.go): runbooks T-167 путь А и
// изолированные сборки; RetentionLoop зовёт retentionPass с общим
// состоянием — его проходы продолжают друг друга.
func RetentionOnce(ctx context.Context, pool RetentionDB, opts RetentionOptions) (RetentionStats, error) {
	return retentionPass(ctx, pool, opts, &retentionState{})
}

// retentionBlocked — вердикт интерлока: глубина q:analyze ≥ порога.
// Ошибка чтения — ошибка (вызывающий пропускает проход: консервативно,
// чистить «наугад» дороже); выключенный интерлок (порог ≤ 0 или
// источник не задан) — не блокирует.
func retentionBlocked(ctx context.Context, o RetentionOptions) (int64, bool, error) {
	if o.AnalyzeWatermark <= 0 || o.Depth == nil {
		return 0, false, nil
	}
	n, err := o.Depth.AnalyzeDepth(ctx)
	if err != nil {
		return 0, false, err
	}
	return n, n >= o.AnalyzeWatermark, nil
}

// retentionSelect — пачка кандидатов после курсора after
// (retentionSelectSQL).
func retentionSelect(ctx context.Context, pool RetentionDB, o RetentionOptions, after retentionCursor) ([]retentionCandidate, error) {
	rows, err := pool.Query(ctx, retentionSelectSQL,
		o.Retention.String(),
		after.checkedAt, after.key.zone, after.key.domain, after.key.sub, after.key.variant,
		o.BatchSize,
	)
	if err != nil {
		return nil, fmt.Errorf("content: retention: выборка кандидатов: %w", err)
	}
	defer rows.Close()

	var out []retentionCandidate
	for rows.Next() {
		var (
			c       retentionCandidate
			variant int16
		)
		if err := rows.Scan(&c.key.zone, &c.key.domain, &c.key.sub, &variant, &c.hash, &c.checkedAt); err != nil {
			return nil, fmt.Errorf("content: retention: выборка кандидатов: чтение: %w", err)
		}
		c.key.variant = variant
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("content: retention: выборка кандидатов: %w", err)
	}
	return out, nil
}

// retentionProbe — точечный зонд ключей ретраев (retentionProbeSQL):
// возвращает всё ещё кандидатов очистки среди предложенных ключей.
// Ключи сортируются — детерминированные аргументы (стиль
// cleanupRefcounts T-020); checkedAt не читается — зонд курсор не
// двигает, его строки чистятся по месту.
func retentionProbe(ctx context.Context, pool RetentionDB, o RetentionOptions, keys []retentionKey) ([]retentionCandidate, error) {
	sort.Slice(keys, func(i, j int) bool { return keys[i].less(keys[j]) })
	zones := make([]int16, len(keys))
	domains := make([]int64, len(keys))
	subs := make([]int16, len(keys))
	variants := make([]int16, len(keys))
	for i, k := range keys {
		zones[i], domains[i], subs[i], variants[i] = k.zone, k.domain, k.sub, k.variant
	}
	rows, err := pool.Query(ctx, retentionProbeSQL,
		zones, domains, subs, variants, o.Retention.String(),
	)
	if err != nil {
		return nil, fmt.Errorf("content: retention: зонд ретраев: %w", err)
	}
	defer rows.Close()

	var out []retentionCandidate
	for rows.Next() {
		var (
			c       retentionCandidate
			variant int16
		)
		if err := rows.Scan(&c.key.zone, &c.key.domain, &c.key.sub, &variant, &c.hash); err != nil {
			return nil, fmt.Errorf("content: retention: зонд ретраев: чтение: %w", err)
		}
		c.key.variant = variant
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("content: retention: зонд ретраев: %w", err)
	}
	return out, nil
}

// retentionPurge — чистка одной пачки в одной транзакции: UPDATE
// subdomain_results (срезание ссылки и jsonb) → уменьшение refcount
// фактически срезанных → удаление contents без ссылок. Крах на любом
// шаге откатывает всё (ТЗ 3.4): строки и refcount не расходятся.
func retentionPurge(ctx context.Context, pool RetentionDB, o RetentionOptions, batch []retentionCandidate) (RetentionStats, error) {
	var st RetentionStats
	zones := make([]int16, len(batch))
	domains := make([]int64, len(batch))
	subs := make([]int16, len(batch))
	variants := make([]int16, len(batch))
	hashes := make([][]byte, len(batch))
	for i, c := range batch {
		zones[i], domains[i], subs[i], variants[i], hashes[i] = c.key.zone, c.key.domain, c.key.sub, c.key.variant, c.hash
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return st, fmt.Errorf("content: retention: транзакция: %w", err)
	}
	// Откат безусловно: после Commit — no-op (стиль T-020/T-044).
	// Прерванная пачка не оставляет половинчатых изменений.
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, retentionPurgeSQL,
		zones, domains, subs, variants, hashes, o.Retention.String())
	if err != nil {
		return st, fmt.Errorf("content: retention: очистка строк: %w", err)
	}
	defer rows.Close()

	// Агрегация срезанных ссылок по хэшу: один контент может ссылаться
	// из многих записей (дедупликация, ТЗ 5.7) — уменьшение кратно.
	refs := make(map[Hash]int32)
	for rows.Next() {
		var hash []byte
		if err := rows.Scan(&hash); err != nil {
			return st, fmt.Errorf("content: retention: очистка строк: чтение: %w", err)
		}
		st.Rows++
		if hash == nil {
			continue // у записи был только jsonb — ссылки нет (T-056)
		}
		st.Refs++
		switch {
		case len(hash) != HashLen:
			// Программная ошибка писателя результатов (T-056): строка
			// уже очищена, ронять пачку нельзя — один кривой хэш блокиро-
			// вал бы retention навсегда (poison pill, стиль ING-P2-4).
			// Ссылку срезали (мусор), refcount по ней не трогаем.
			st.BadHashes++
		default:
			var h Hash
			copy(h[:], hash)
			refs[h]++
		}
	}
	if err := rows.Err(); err != nil {
		return st, fmt.Errorf("content: retention: очистка строк: %w", err)
	}

	if len(refs) > 0 {
		if err := retentionDecrease(ctx, tx, refs, &st); err != nil {
			return st, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return st, fmt.Errorf("content: retention: фиксация: %w", err)
	}
	st.Batches = 1
	return st, nil
}

// retentionDecrease — шаги contents транзакции пачки: уменьшение
// refcount по агрегированным хэшам (retentionDecrefSQL) и удаление
// строк, лишившихся последней ссылки (retentionDeleteSQL). Отсутствие
// строки contents у срезанной ссылки — расхождение учёта (Missing):
// счётчик для алерта, не ошибка пачки (poison pill, как BadHashes).
func retentionDecrease(ctx context.Context, tx pgx.Tx, refs map[Hash]int32, st *RetentionStats) error {
	list := make([]Hash, 0, len(refs))
	for h := range refs {
		list = append(list, h)
	}
	// Детерминированный порядок аргументов — воспроизводимые пачки
	// (стиль cleanupRefcounts T-020).
	sort.Slice(list, func(i, j int) bool {
		return compareHash(list[i], list[j]) < 0
	})
	hashes := make([][]byte, len(list))
	counts := make([]int32, len(list))
	for i, h := range list {
		hashes[i], counts[i] = h[:], refs[h]
	}

	rows, err := tx.Query(ctx, retentionDecrefSQL, hashes, counts)
	if err != nil {
		return fmt.Errorf("content: retention: уменьшение refcount: %w", err)
	}
	defer rows.Close()
	updated := make(map[Hash]struct{}, len(list))
	var dead [][]byte
	for rows.Next() {
		var (
			hash []byte
			refc int32
			clen int32
		)
		if err := rows.Scan(&hash, &refc, &clen); err != nil {
			return fmt.Errorf("content: retention: уменьшение refcount: чтение: %w", err)
		}
		var h Hash
		copy(h[:], hash)
		updated[h] = struct{}{}
		if refc <= 0 {
			dead = append(dead, hash)
			st.FreedBytes += int64(clen)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("content: retention: уменьшение refcount: %w", err)
	}
	// Срезанные ссылки, не найденные в contents: расхождение учёта
	// (ссылка без счётчика ломала бы инвариант T-044) — refcount по ним
	// не менялся, счётчик для алерта; не ошибка пачки (poison pill).
	st.Missing += int64(len(list) - len(updated))

	if len(dead) > 0 {
		tag, err := tx.Exec(ctx, retentionDeleteSQL, dead)
		if err != nil {
			return fmt.Errorf("content: retention: удаление contents: %w", err)
		}
		st.Contents += tag.RowsAffected()
	}
	return nil
}

// compareHash — сравнение хэшей лексикографически (порядок пачки).
func compareHash(a, b Hash) int {
	for i := range a {
		switch {
		case a[i] < b[i]:
			return -1
		case a[i] > b[i]:
			return 1
		}
	}
	return 0
}

// RetentionLoop — фоновый цикл retention (T-166): проходы retentionPass
// по ticker (первый — сразу: дренаж накопленного за простоем — стиль
// CompactLoop/CleanupDeleted) до отмены ctx, возврат nil — контракт
// OnRun каркаса сервисов (T-012). Состояние проходов (retentionState,
// T-185) — курсор дренажа по времени и список ретраев — переживает
// проходы: строка посещается один раз за жизнь, часовой проход
// продолжает с места остановки (стоимость ∝ притоку, не размеру
// таблицы); runbook-точки входа см. retentiondrain.go. Retention = 0 —
// retention выключен: цикл не поднимается вовсе (Info в лог). Ошибка
// прохода не останавливает цикл: Warn, очередной проход повторит —
// проходы идемпотентны (ТЗ 3.4). Состояние трогает только цикл
// (одна горутина тикера) — блокировки не нужны.
func RetentionLoop(ctx context.Context, pool RetentionDB, opts RetentionOptions) error {
	if pool == nil {
		return errors.New("content: retention: пул не задан")
	}
	o := opts.withDefaults()
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	if o.Retention <= 0 {
		log.Info("retention контента выключен (content.retention = 0)")
		return nil
	}
	log.Info("цикл retention контента запущен",
		"retention", o.Retention.String(),
		"interval", o.Interval.String(),
		"batch", o.BatchSize,
		"analyze_watermark", o.AnalyzeWatermark,
		"queue_guard", o.Pending != nil,
		"retry_limit", DefaultRetentionRetryLimit)

	state := &retentionState{}
	pass := func() {
		start := time.Now()
		stats, err := retentionPass(ctx, pool, o, state)
		if err != nil {
			// Отмена во время прохода — часть остановки, не тревога.
			if ctx.Err() == nil {
				log.Warn("проход retention контента", "err", err)
			}
			return
		}
		if stats.Skipped {
			// Интерлок: анализатор не успевает, тела ещё нужны — алерт
			// по метрике пропусков (retentionmetrics.go).
			log.Warn("проход retention контента пропущен: глубина q:analyze у порога",
				"depth", stats.Depth, "watermark", o.AnalyzeWatermark)
		}
		if stats.SkippedPending {
			// Защита очередей: чтение непотреблённых q:render/q:analyze не
			// удалось — записи, ожидающие потребителей, не тронуты; алерт
			// по метрике crawler_content_retention_pending_errors_total.
			// rows_done — пачки, зафиксированные до точки ошибки (пропуск
			// касается остатка прохода, сделанное остаётся в БД).
			log.Warn("проход retention контента пропущен: ошибка чтения q:render/q:analyze",
				"rows_done", stats.Rows)
		}
		if n := stats.Deferred[queue.StreamRender]; n > 0 {
			log.Warn("retention: записи ожидают q:render — не тронуты", "deferred", n)
		}
		if n := stats.Deferred[queue.StreamAnalyze]; n > 0 {
			log.Warn("retention: записи ожидают q:analyze — не тронуты", "deferred", n)
		}
		if stats.BadHashes > 0 || stats.Missing > 0 {
			log.Warn("retention: аномалии учёта контента",
				"bad_hashes", stats.BadHashes, "missing_refs", stats.Missing)
		}
		if stats.Rows > 0 {
			log.Info("проход retention контента",
				"rows", stats.Rows, "refs", stats.Refs,
				"contents", stats.Contents, "freed_bytes", stats.FreedBytes,
				"batches", stats.Batches)
		}
		// Итог прохода — потребителям (метрики): после лога.
		if o.OnPass != nil {
			o.OnPass(stats, time.Since(start))
		}
	}

	pass()
	ticker := time.NewTicker(o.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			pass()
		}
	}
}

// Scheduler: повторная проверка temp_error (T-031; ТЗ 5.3, 6.2).
//
// Домен, чья проверка завершилась временной ошибкой (status = temp_error,
// ТЗ 6.2: SERVFAIL/таймауты, «требуется повтор»), перепроверяется не сразу,
// а через temp_error_retry_after (по умолчанию 24 ч, ТЗ 5.3) и не более
// N раз подряд; после исчерпания попыток домен получает итоговый статус
// по результату последней проверки.
//
// Проход RetryOnce пачкой до opts.BatchSize выбирает кандидатов — домены
// в статусе temp_error, чья проверка завершена (check_state = done, ТЗ 7)
// и last_checked_at старше temp_error_retry_after, — и делит их по
// счётчику попыток domains.temp_attempts (миграция 000005) на два решения,
// каждое — в СОБСТВЕННОЙ транзакции (порядок блокировок, retryDecide;
// назначение повтора — два оператора: декремент корзин счётчиков вынесен
// из оператора зон, T-267):
//
//   - попыток меньше N — назначается повтор: check_state → pending
//     (домена перевыберет диспетчер T-027 и поставит в q:dns, T-028),
//     счётчик увеличивается на единицу;
//   - попыток N или больше — фиксируется итоговый статус
//     delegated_no_address, счётчик сбрасывается в ноль.
//
// Итоговый статус — delegated_no_address (код 2, ТЗ 6.2): последняя
// проверка закончилась SERVFAIL/таймаутом, то есть адресов для HTTP-этапа
// не получено — это терминальная ветка «нет адресов» алгоритма ТЗ 7
// («нет адресов → status=delegated_no_address, done»). not_delegated
// отвергнут: он кодирует именно NXDOMAIN апекса (ТЗ 5.4 п. 1, RFC 8020),
// а ТЗ явно различает NXDOMAIN и SERVFAIL/таймаут; новых статусов набор
// не расширяет — семь статусов подтверждены заказчиком (ТЗ 13.1).
// Ошибка классификации самокорректируется регулярным циклом: домен
// остаётся в done и перепроверяется по recheck_interval (T-030, 6 мес).
//
// Счётчик temp_attempts без смены схемы не выразить: check_state —
// перечисление жизненного цикла, flags — битовые булевы признаки,
// dns_mask — маска субдоменов, метки времени не считают попытки, истории
// результатов нет (ТЗ 3.5, перезапись). Миграция 000005 — минимальная:
// одна smallint-колонка. Сброс счётчика при успешной (не temp_error)
// проверке — контракт DNS-этапа (T-036+): новый эпизод temp_error
// начинается с нуля; до его реализации счётчик между эпизодами может
// остаться ненулевым — эффект консервативен (меньше повторов).
//
// Wiring в cmd/scheduler — поздняя задача (OnRun каркаса T-012).

package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/db"
	"crawler/internal/model"
)

const (
	// DefaultRetryAfter — окно повтора temp_error (ТЗ 5.3:
	// temp_error_retry_after, 24 часа).
	DefaultRetryAfter = 24 * time.Hour

	// DefaultRetryMaxRetries — максимум повторов подряд (ТЗ 5.3: «не более
	// N раз подряд»). Число ТЗ не задаёт; 3 — как счётчик попыток доставки
	// очереди (план T-025): единая политика повторов системы.
	DefaultRetryMaxRetries = 3

	// DefaultRetryBatch — лимит пачки: доменов на один проход, в стиле
	// пачек планировщика (ТЗ 5.3: 1000–10 000) и фоновой очистки T-020.
	DefaultRetryBatch = 1000

	// DefaultRetryInterval — интервал проходов цикла RetryTempErrors при
	// настройках по умолчанию. ТЗ частоту проходов не задаёт: с окном
	// повтора 24 часа часовой проход назначает повтор не позже чем через
	// час после готовности домена; если retry_after настроен меньше часа,
	// интервал ужимается до него (withDefaults), чтобы цикл не «переспал»
	// собственное окно. Стационар без зрелых кандидатов держит guard
	// корзин зрелости (T-202, retryMatureSQL — уточнение T-191): часовой
	// тик стоит одну дешёвую сумму строк счётчика, тяжёлая выборка пачки
	// не выполняется вовсе; шторм когорты (массовый temp_error-эпизод)
	// дренируется проходами подряд (drain), темп определяют UPDATE пачки,
	// а не выборка — seq scan с ранним остановом LIMIT, закреплённый в
	// транзакции выборки (retryCandidates, T-191).
	DefaultRetryInterval = time.Hour
)

// SQL прохода. unnest-пачки — стиль пакетных UPDATE internal/db (ТЗ 6:
// «запись пачками … UPDATE … FROM unnest(...)»); охраны в WHERE —
// перечитывание состояния на момент апдейта (гонки с T-027/T-030/T-018
// не ломают инвариант «один инкремент на эпизод»), RETURNING отдаёт
// только фактически затронутые строки — счётчики прохода честные.
const (
	// retrySelectSQL — кандидаты повтора: temp_error после завершённой
	// проверки (check_state = done — точка инкремента: одно назначение
	// повтора на один эпизод проверки; pending уже ждёт выборки диспетчера,
	// queued в работе, ТЗ 5.3), окно — по last_checked_at. NULL
	// last_checked_at не выбирается: «ещё не проверялся» — не повод для
	// повтора. ORDER BY отсутствует сознательно (T-191, прецедент T-181):
	// порядок кандидатов не важен — деление пачки по temp_attempts не
	// зависит от порядка, охраны retryRequeueSQL/retryFinalizeSQL
	// перепроверяют статус и состояние на момент записи (идемпотентность
	// ТЗ 3.4), повторный проход безопасен; сортировка держала бы индексный
	// доступ к таблице. Точность окна — сутки: last_checked_at имеет тип
	// date (ТЗ 6.1).
	//
	// Индекса у прохода нет сознательно (T-104): предикат сводится к
	// status = temp_error, а индекс с status в ключе или предикате ломал
	// бы HOT массовых апдейтов статуса без смены check_state — применение
	// delete-файлов (apply_delete) и фиксация итогового статуса
	// (retryFinalizeSQL). Но и без status в индексе seq scan не бесплатен:
	// планировщик садится на частичный domains_recheck_idx (миграция
	// 000008) — предикат check_state = done доказуем, диапазон по ведущему
	// last_checked_at кажется узким, а слепая оценка (гистограмма
	// last_checked_at: 92 % NULL, волна проверок моложе статистики)
	// обещает планировщику тысячи строк; реально окно «старше 24 ч»
	// покрывает почти все done-строки, и диапазонный скан вырождается в
	// марафон по нескольким миллионам строк с heap-визитом каждой (запрос
	// №1 журнала медленных 25.10: медиана 74,8 с, ≈3,6 ч времени БД за
	// окно). Форма плана закреплена: seq scan с ранним остановом LIMIT —
	// доли секунды, замер 195× (T-191) — SET LOCAL в транзакции выборки
	// (retryCandidates, образец — renderSelectPending T-181).
	retrySelectSQL = "SELECT d.zone_id, d.id, d.temp_attempts FROM domains d " +
		"WHERE d.status = $1 " +
		"AND d.check_state = $2 " +
		"AND d.last_checked_at < $3 " +
		"LIMIT $4"

	// retryPlanNoIndexScanSQL/retryPlanNoBitmapScanSQL/retryPlanNoIOSQL —
	// закрепление формы плана выборки кандидатов (T-191, прецедент T-181):
	// SET LOCAL в транзакции retryCandidates отключает index scan, bitmap
	// scan и index-only scan до оператора выборки — закрыты все индексные
	// формы доступа к domains (PK и оба частичных), seq scan с ранним
	// остановом LIMIT гарантирует пачку за доли секунды независимо от
	// статистики (оценке планировщика верить нельзя — см.
	// retrySelectSQL). Действие SET LOCAL — только эта транзакция,
	// остальные запросы пула не затронуты.
	retryPlanNoIndexScanSQL  = "SET LOCAL enable_indexscan = off"
	retryPlanNoBitmapScanSQL = "SET LOCAL enable_bitmapscan = off"
	retryPlanNoIOSQL         = "SET LOCAL enable_indexonlyscan = off"

	// retryRequeueSQL — назначение повтора: check_state → pending (домена
	// перевыберет диспетчер T-027), счётчик попыток + 1. Охрана
	// status = temp_error AND check_state = done: домен, успевший уйти
	// из done мимо прохода (recheck T-030, возврат lease T-029, воскрешение
	// add-файлом T-018), не инкрементируется повторно. Дельты счётчиков
	// зон (checked −n / pending +n) — CTE zc того же оператора по
	// RETURNING zone_id (T-178, ТЗИ §6.2: предикат пинирует старое
	// состояние done); внешний SELECT — ключи переведённых строк с датой
	// эпизода (last_checked_at — адресат хвостового декремента корзин
	// зрелости, retryRequeueMatureSQL). Оператор строго «domains →
	// zones» (T-267): декремент корзин выведен из него ВТОРЫМ оператором
	// транзакции решения — нессылаемые DML-CTE зон и счётчика одного
	// оператора исполняются в непредсказуемом порядке (документация PG,
	// оговорка в zonestats.go; инцидент 07.11 — ребро «счётчики → зоны»
	// против зонодержателей, runbooks.md §2.8), операторы же транзакции
	// исполняются в текстовом порядке; зоны удерживаются до Commit —
	// конвенция «domains → zones → counters» соблюдена, ребро
	// «счётчики → domains» (устранено разделением транзакций решений,
	// 15ff3a3) не возвращается: после декремента транзакция новых строк
	// domains не захватывает.
	retryRequeueSQL = "WITH upd AS (UPDATE domains d " +
		"SET check_state = $3, temp_attempts = d.temp_attempts + 1 " +
		"FROM unnest($1::int2[], $2::int8[]) AS k(zone_id, domain_id) " +
		"WHERE d.zone_id = k.zone_id AND d.id = k.domain_id " +
		"AND d.status = $4 AND d.check_state = $5 " +
		"RETURNING d.zone_id, d.id, d.last_checked_at), " +
		"zc AS (UPDATE zones AS z " +
		"SET domains_checked = z.domains_checked - c.n, " +
		"domains_pending = z.domains_pending + c.n " +
		"FROM (SELECT zone_id, count(*) AS n FROM upd GROUP BY zone_id) AS c " +
		"WHERE z.id = c.zone_id) " +
		"SELECT u.zone_id, u.id, u.last_checked_at FROM upd AS u"

	// retryRequeueMatureSQL — хвостовой декремент корзин зрелости
	// назначения повтора (T-202/T-267): второй оператор транзакции
	// решения ПОСЛЕ перевода (retryRequeueSQL: upd+zc), по датам
	// эпизодов переведённых строк — RETURNING первого оператора
	// (перевод в pending завершает кандидату: момент зрелости выводится
	// из last_checked_at, ключ тот же, что у инкремента записи
	// результата — current_date её оператора; рассинхрон ключей лишил
	// бы декремент адресата). Параметр — пачка дат unnest($1::date[]);
	// ORDER BY детерминирует захват строк корзин по возрастанию даты
	// (= имени: старые → вчерашняя) — общий порядок с проходом monitor
	// (очистка старых → вчерашняя → сегодняшняя), без AB-BA цикла на
	// смешанных датах пачки (ревью T-202, P2). В той же транзакции,
	// что и перевод: сбой декремента откатывает пару целиком (§3.4),
	// кандидаты остаются кандидатами следующего прохода — в отличие от
	// разнесённых autocommit'ами операторов записи результата DNS-этапа
	// (dnsworker/result.go, окно ±1 до реконсиляции); пустая пачка
	// (охрана отбила всех) оператор не выполняется вовсе.
	retryRequeueMatureSQL = "INSERT INTO crawler_counters (name, value, updated_at) " +
		"SELECT 'retry_mature:' || to_char(d, 'YYYY-MM-DD'), -count(*), now() " +
		"FROM unnest($1::date[]) AS d " +
		"GROUP BY d ORDER BY d " +
		"ON CONFLICT (name) DO UPDATE " +
		"SET value = crawler_counters.value + EXCLUDED.value, updated_at = now()"

	// retryFinalizeSQL — исчерпание попыток: итоговый статус по последней
	// проверке — delegated_no_address (см. комментарий в начале файла),
	// счётчик в ноль: следующий эпизод temp_error начнётся с чистого
	// счётчика. Охрана та же, что у назначения повтора. Дельт счётчиков
	// зон нет: check_state остаётся done, status-распределение
	// инкрементами не ведётся — его пишет реконсиляция monitor (ТЗИ
	// §13.3, T-178). CTE rm декрементирует корзину зрелости (T-202):
	// финализация, как и назначение повтора, завершает кандидату — без
	// декремента корзина держала бы guard открытым после дренажа (холостые
	// seq scan каждый тик до прохода реконсиляции); ORDER BY —
	// детерминация захвата строк корзин (ревью T-202, P2). Декремент
	// ОСТАВЛЕН в самом операторе сознательно (T-267 выносил ребро
	// «счётчики → зоны» горячего requeue): rm ссылается на upd —
	// единственная CTE, исполнение гарантированно после перевода; зон
	// оператор не трогает — порядок блокировок «domains → counters»
	// без ребра против зонодержателей, отдельный оператор ничего бы
	// не изменил.
	retryFinalizeSQL = "WITH upd AS (UPDATE domains d " +
		"SET status = $3, temp_attempts = 0 " +
		"FROM unnest($1::int2[], $2::int8[]) AS k(zone_id, domain_id) " +
		"WHERE d.zone_id = k.zone_id AND d.id = k.domain_id " +
		"AND d.status = $4 AND d.check_state = $5 " +
		"RETURNING d.zone_id, d.id, d.last_checked_at), " +
		"rm AS (INSERT INTO crawler_counters (name, value, updated_at) " +
		"SELECT 'retry_mature:' || to_char(u.last_checked_at, 'YYYY-MM-DD'), -count(*), now() " +
		"FROM upd AS u GROUP BY u.last_checked_at " +
		"ORDER BY u.last_checked_at " +
		"ON CONFLICT (name) DO UPDATE " +
		"SET value = crawler_counters.value + EXCLUDED.value, updated_at = now()) " +
		"SELECT u.zone_id, u.id FROM upd AS u"
)

// retryMatureSQL — guard прохода суммой корзин зрелости temp_error
// (T-202, уточнение guard T-191: счётчик status_temp_error не отличал
// зрелых кандидатов от незрелых — 543 тыс. temp_error при пустом окне
// зрелости держали seq scan без раннего останова на каждом часовом тике,
// медиана 51 с / max 239 с после фикса T-191). Корзина
// crawler_counters['retry_mature:YYYY-MM-DD'] — temp_error-эпизоды
// суток (дата эпизода = last_checked_at записи результата): +1 пишут
// инкременты записи результата (httpworker/record.go и терминальная
// temp_error-ветка DNS-этапа dnsworker/result.go — отдельным
// autocommit-оператором после записи, T-267, — оба источника
// эпизодов), −n — решения ретрая: назначение повтора хвостовым
// оператором retryRequeueMatureSQL, финализация — CTE rm самого
// оператора (кандидата исчерпана); авторитет — реконсиляция monitor
// (FILTER-позиции прохода, T-202). Сумма корзин прошедших суток —
// число зрелых кандидатов: last_checked_at — date (миграция 000001),
// окно «старше 24 ч» по date-колонке зреет когортами в полночь, поэтому
// ключ корзины — дата эпизода (эквивалент «момента созревания now+24 ч,
// округлённого до часа»: при date-точности колонки все эпизоды суток
// зреют вместе в полночь следующего дня — часовая гранулярность
// вырождается в суточную, формат наследует T-179; тот же ключ выводим и
// инкрементом (current_date), и декрементом (last_checked_at) —
// рассинхрон ключей лишил бы декремент адресата). Диапазон по
// PK-префиксу 'retry_mature:' лексикографически отсекает корзину
// текущих суток (name < …current_date: сегодняшние эпизоды ещё
// незрелые) — имя дня вычисляет БД, Go-часы в границе не участвуют
// (стиль T-179).
// coalesce — строк корзин может не быть вовсе (до первого инкремента
// или прохода реконсиляции): честный ноль, а не ошибка — в отличие от
// status_temp_error строки корзин nobody не гарантирует, их отсутствие
// и есть «зрелых нет». Значение ≤ 0 — зрелых нет: тяжёлая выборка
// (seq scan по domains) не выполняется вовсе, стационар стоит одну
// сумму индексного диапазона из пары строк.
var retryMatureSQL = "SELECT coalesce(sum(value), 0) FROM crawler_counters " +
	"WHERE name >= 'retry_mature:' " +
	"AND name < 'retry_mature:' || to_char(current_date, 'YYYY-MM-DD')"

// RetryDB — пул для прохода: db.DB, из которого можно начать транзакцию.
// Решения пачки (назначение повтора и итоговый статус) идут РАЗДЕЛЬНЫМИ
// транзакциями (retryDecide): каждая по отдельности соблюдает конвенцию
// порядка блокировок «domains → zones → counters, счётчики — последними»
// (docs/schema.md, раздел crawler_counters); общая транзакция захватывала
// бы domains-строки финализации, уже держа зоны и корзины счётчиков, —
// ребро «счётчики → domains» замыкало цикл дедлока 40P01 с транзакциями
// записи результата http-worker (инцидент retry-волны: пачки по 1000,
// десятки корзин retry_mature до конца tx). Назначение повтора
// выполняется в своей транзакции ДВУМЯ операторами (T-267): перевод
// (upd+zc), затем декремент корзин (retryRequeueMatureSQL) — операторы
// транзакции исполняются в текстовом порядке, зоны удерживаются до
// Commit. Атомарность пачки не критична: охраны обоих UPDATE
// идемпотентны, незавершённая половина безопасно подбирается следующим
// проходом (ТЗ 3.4);
// *pgxpool.Pool удовлетворяет интерфейсу.
type RetryDB interface {
	db.DB
	Begin(ctx context.Context) (pgx.Tx, error)
}

// compile-time: реальный пул подходит под интерфейс.
var _ RetryDB = (*pgxpool.Pool)(nil)

// RetryOptions — параметры прохода; нулевые значения заменяются
// дефолтами (withDefaults, стиль ingest.CleanupOptions).
type RetryOptions struct {
	// RetryAfter — окно повтора temp_error: кандидаты — домены с
	// last_checked_at старше (DefaultRetryAfter; конфиг
	// scheduler.temp_error_retry_after, ТЗ 5.3).
	RetryAfter time.Duration
	// MaxRetries — максимум повторов подряд (DefaultRetryMaxRetries;
	// конфиг scheduler.temp_error_max_retries, ТЗ 5.3: «не более N раз
	// подряд»).
	MaxRetries int
	// BatchSize — максимум доменов в одном проходе (DefaultRetryBatch);
	// лимит пачки задаёт SELECT кандидатов.
	BatchSize int
	// Interval — период проходов цикла RetryTempErrors; на разовый
	// RetryOnce не влияет. Ноль/отрицательное — min(RetryAfter,
	// DefaultRetryInterval), см. DefaultRetryInterval.
	Interval time.Duration
	// Log — логгер цикла RetryTempErrors; nil — записи подавлены
	// (slog.DiscardHandler). RetryOnce не логирует — итог возвращается
	// RetryResult.
	Log *slog.Logger
}

// withDefaults заменяет неположительные значения дефолтами.
func (o RetryOptions) withDefaults() RetryOptions {
	if o.RetryAfter <= 0 {
		o.RetryAfter = DefaultRetryAfter
	}
	if o.MaxRetries <= 0 {
		o.MaxRetries = DefaultRetryMaxRetries
	}
	if o.BatchSize <= 0 {
		o.BatchSize = DefaultRetryBatch
	}
	if o.Interval <= 0 {
		// Не дольше собственного окна повтора: при коротком retry_after
		// (тесты, эксперименты) цикл обязан просыпаться чаще окна.
		o.Interval = min(o.RetryAfter, DefaultRetryInterval)
	}
	return o
}

// RetryResult — итог одного прохода RetryOnce.
type RetryResult struct {
	// Requeued — домены, которым назначен повтор (check_state → pending,
	// temp_attempts + 1); после охраны — только фактически переведённые.
	Requeued int
	// Finalized — домены с зафиксированным итоговым статусом
	// (delegated_no_address, temp_attempts → 0).
	Finalized int
}

// retryCandidate — кандидат прохода: ключ домена и счётчик попыток.
type retryCandidate struct {
	zoneID   int16
	id       int64
	attempts int16
}

// RetryOnce выполняет один проход повторных проверок (ТЗ 5.3, 6.2):
// до opts.BatchSize доменов в статусе temp_error с завершённой
// проверкой и last_checked_at старше RetryAfter получают либо
// назначенный повтор (check_state → pending, temp_attempts + 1 — попыток
// меньше MaxRetries), либо итоговый статус delegated_no_address (попытки
// исчерпаны, temp_attempts → 0). Решения пачки идут РАЗДЕЛЬНЫМИ
// транзакциями (retryDecide): каждая соблюдает порядок блокировок
// «domains → zones → counters» — общая транзакция захватывала бы
// domains-строки финализации, уже держа зоны и корзины retry_mature
// (ребро «счётчики → domains», цикл дедлока 40P01 с записью результата
// http-worker); назначение повтора — ДВА оператора своей транзакции
// (перевод upd+zc, затем декремент корзин по датам переведённых строк,
// T-267 — порядок нессылаемых DML-CTE одного оператора PG не
// гарантирует); сбой между половинами безопасен — охраны обоих UPDATE
// идемпотентны, незавершённая половина подберётся следующим проходом
// (ТЗ 3.4). До выборки — guard суммой корзин зрелости
// crawler_counters['retry_mature:…'] прошедших суток (T-202,
// retryMatureCount): нулевая сумма возвращает пустой результат без
// тяжёлой выборки. Повторный проход безопасен (ТЗ 3.4): домены в
// pending больше не кандидаты, охраны в UPDATE перепроверяют состояние
// на момент записи.
func RetryOnce(ctx context.Context, pool RetryDB, opts RetryOptions) (RetryResult, error) {
	if pool == nil {
		return RetryResult{}, errors.New("scheduler: повторные проверки temp_error: пул не задан")
	}
	o := opts.withDefaults()

	// Guard корзинами зрелости (T-202): до тяжёлой выборки — сумма корзин
	// прошедших суток (retryMatureCount); 0 → пустой результат без выборки
	// и транзакции (drain корректно останавливается). Отрицательная сумма —
	// видимый дрейф декрементов мимо инкрементов (кандидаты, ушедшие мимо
	// ретрая: удаление temp_error-домена delete-файлом, возрастная
	// перепроверка T-030): реконсиляция выровняет, а guard обязан молчать —
	// тихий баг счётчика не должен молотить БД (наследие T-191).
	mature, err := retryMatureCount(ctx, pool)
	if err != nil {
		return RetryResult{}, err
	}
	if mature <= 0 {
		return RetryResult{}, nil
	}

	candidates, err := retryCandidates(ctx, pool, o.BatchSize, o.RetryAfter)
	if err != nil {
		return RetryResult{}, err
	}
	if len(candidates) == 0 {
		// Корзины видят зрелых, выборка — нет (дрейф корзин: кандидат ушёл
		// мимо инкрементов/декрементов, ЛИБО редкие зрелые рассеяны и ранний
		// останов LIMIT их не догнал): транзакции решений не открываются,
		// один холостой seq scan за тик до ближайшей реконсиляции —
		// остаточный случай TZ T-191 §4.3, суженный T-202 с «всех temp_error»
		// до «суммы корзин».
		return RetryResult{}, nil
	}

	var (
		requeueZones []int16
		requeueIDs   []int64
		finalZones   []int16
		finalIDs     []int64
	)
	for _, c := range candidates {
		if int(c.attempts) < o.MaxRetries {
			requeueZones = append(requeueZones, c.zoneID)
			requeueIDs = append(requeueIDs, c.id)
		} else {
			finalZones = append(finalZones, c.zoneID)
			finalIDs = append(finalIDs, c.id)
		}
	}

	var res RetryResult
	if len(requeueZones) > 0 {
		n, err := retryDecide(ctx, pool, retryRequeueSQL, retryRequeueMatureSQL, "назначение повтора", requeueZones, requeueIDs,
			int16(model.CheckStatePending), int16(model.StatusTempError), int16(model.CheckStateDone))
		if err != nil {
			return RetryResult{}, err
		}
		res.Requeued = n
	}
	if len(finalZones) > 0 {
		n, err := retryDecide(ctx, pool, retryFinalizeSQL, "", "итоговый статус", finalZones, finalIDs,
			int16(model.StatusDelegatedNoAddress), int16(model.StatusTempError), int16(model.CheckStateDone))
		if err != nil {
			return RetryResult{}, err
		}
		res.Finalized = n
	}
	return res, nil
}

// retryDecide — одно решение пачки (назначение повтора или итоговый
// статус) в собственной транзакции. Разделение транзакций — инвариант
// порядка блокировок (docs/schema.md, раздел crawler_counters): ДВА
// решения в одной транзакции захватывают domains-строки второй половины,
// уже держа зоны и корзины retry_mature:{даты} первой, — ребро
// «счётчики → domains» против транзакций записи результата http-worker
// и прохода реконсиляции monitor (обе идут «domains → … → counters» и
// до счётчиков ждут своих domains-строк) замыкало цикл дедлока 40P01:
// 9 случаев за 3 ч на retry-волне, задача q:http ределиверилась полным
// повторным HTTP-эпизодом. Раздельные транзакции каждую возвращают
// конвенции, циклы через http-worker рвутся.
// Назначение повтора выполняет в своей транзакции ДВА оператора
// (T-267, matureSQL = retryRequeueMatureSQL): перевод (retryRequeueSQL:
// upd → zc), затем хвостовой декремент корзин зрелости по ключам
// (датам эпизодов) первого — операторы транзакции исполняются в
// текстовом порядке, в отличие от нессылаемых DML-CTE одного оператора
// (порядок PG не гарантирует, оговорка в zonestats.go; смешение zc/rm
// одного оператора создавало ребро «счётчики → зоны» — инцидент
// 07.11, runbooks.md §2.8). Зоны удерживаются до Commit — порядок
// «domains → zones → counters» тот же, ребро «счётчики → domains»
// не возвращается: после декремента транзакция новых строк domains
// не захватывает. matureSQL = "" — решение без декремента
// (финализация: rm в самом операторе, ссылается на upd). Декремент
// выполняется только по непустому множеству ключей (охрана отбила
// всех — корзины не трогаем).
// Сбой любого оператора откатывает половину целиком (перевод и
// декремент — одна транзакция, ТЗ 3.4): прерванная половина не
// оставляет половинчатых изменений, кандидаты остаются кандидатами
// следующего прохода (идемпотентность); зафиксированная половина
// остаётся (переведённые не кандидаты). Ошибка половины возвращает
// ошибку прохода — RetryResult отбрасывается, дренаж RetryTempErrors
// продолжит следующим тиком.
func retryDecide(ctx context.Context, pool RetryDB, sql, matureSQL, kind string,
	zoneIDs []int16, domainIDs []int64, set, guardStatus, guardState int16) (int, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("scheduler: повторные проверки temp_error: транзакция решения (%s): %w", kind, err)
	}
	// Откат безусловно: после Commit — no-op (стиль T-044). Прерванная
	// половина не оставляет половинчатых изменений внутри себя (ТЗ 3.4).
	defer func() { _ = tx.Rollback(ctx) }()

	keys, err := retryUpdate(ctx, tx, sql, kind, zoneIDs, domainIDs, set, guardStatus, guardState)
	if err != nil {
		return 0, err
	}
	if matureSQL != "" && len(keys) > 0 {
		days := make([]time.Time, len(keys))
		for i, k := range keys {
			days[i] = k.day
		}
		if _, err := tx.Exec(ctx, matureSQL, days); err != nil {
			return 0, fmt.Errorf("scheduler: повторные проверки temp_error: %s: декремент корзин зрелости: %w", kind, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("scheduler: повторные проверки temp_error: фиксация решения (%s): %w", kind, err)
	}
	return len(keys), nil
}

// retryCandidates выбирает до limit кандидатов повтора (retrySelectSQL) —
// единая точка тяжёлой выборки прохода: ключи доменов и их счётчики
// попыток для деления пачки. Выборка идёт в явной транзакции с SET LOCAL
// enable_indexscan/bitmapscan/indexonlyscan = off (T-191, образец —
// renderSelectPending T-181): закрепление формы плана — seq scan по
// domains с ранним остановом LIMIT; без закрепления планировщик садится
// на частичный domains_recheck_idx и вырождает окно повтора в марафон
// (медиана 74,8 с на замере 25.10 — комментарий retrySelectSQL).
// Транзакция — только оболочка закрепления: SELECT читает
// зафиксированные данные, фиксация/откат по содержанию равнозначны.
func retryCandidates(ctx context.Context, pool RetryDB, limit int, retryAfter time.Duration) ([]retryCandidate, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("scheduler: повторные проверки temp_error: транзакция выборки: %w", err)
	}
	// Откат безусловно: после Commit — no-op (стиль T-044).
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, retryPlanNoIndexScanSQL); err != nil {
		return nil, fmt.Errorf("scheduler: повторные проверки temp_error: закрепление плана: %w", err)
	}
	if _, err := tx.Exec(ctx, retryPlanNoBitmapScanSQL); err != nil {
		return nil, fmt.Errorf("scheduler: повторные проверки temp_error: закрепление плана: %w", err)
	}
	if _, err := tx.Exec(ctx, retryPlanNoIOSQL); err != nil {
		return nil, fmt.Errorf("scheduler: повторные проверки temp_error: закрепление плана: %w", err)
	}

	cutoff := time.Now().Add(-retryAfter)
	rows, err := tx.Query(ctx, retrySelectSQL,
		int16(model.StatusTempError), int16(model.CheckStateDone), cutoff, limit)
	if err != nil {
		return nil, fmt.Errorf("scheduler: повторные проверки temp_error: выборка кандидатов: %w", err)
	}
	defer rows.Close()

	candidates := make([]retryCandidate, 0, limit)
	for rows.Next() {
		var c retryCandidate
		if err := rows.Scan(&c.zoneID, &c.id, &c.attempts); err != nil {
			return nil, fmt.Errorf("scheduler: повторные проверки temp_error: чтение кандидатов: %w", err)
		}
		candidates = append(candidates, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scheduler: повторные проверки temp_error: выборка кандидатов: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("scheduler: повторные проверки temp_error: фиксация выборки: %w", err)
	}
	return candidates, nil
}

// retryMatureCount — guard прохода повторных проверок (T-202): сумма
// корзин зрелости crawler_counters['retry_mature:…'] прошедших суток
// (retryMatureSQL; ведут инкременты записи результата, декременты
// ретрая и реконсиляция monitor — guard читает зрелых кандидатов, а не
// весь статус temp_error, как T-191). coalesce в SQL гарантирует одну
// строку даже без строк корзин (честный ноль): пустой набор — обрыв
// протокола, а не «корзин нет», и обязан быть ошибкой.
func retryMatureCount(ctx context.Context, pool RetryDB) (int64, error) {
	rows, err := pool.Query(ctx, retryMatureSQL)
	if err != nil {
		return 0, fmt.Errorf("scheduler: повторные проверки temp_error: корзины зрелости: %w", err)
	}
	defer rows.Close()
	var n int64
	if !rows.Next() {
		// Сначала сбой чтения: обрыв соединения — не «пустая сумма»
		// (coalesce в SQL отвечает строкой всегда).
		if err := rows.Err(); err != nil {
			return 0, fmt.Errorf("scheduler: повторные проверки temp_error: корзины зрелости: %w", err)
		}
		return 0, fmt.Errorf("scheduler: повторные проверки temp_error: корзины зрелости: пустой ответ агрегата")
	}
	if err := rows.Scan(&n); err != nil {
		return 0, fmt.Errorf("scheduler: повторные проверки temp_error: корзины зрелости: чтение: %w", err)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("scheduler: повторные проверки temp_error: корзины зрелости: %w", err)
	}
	return n, nil
}

// retryKey — ключ переведённой строки пачки: зона, домен и дата
// эпизода (last_checked_at) — адресат хвостового декремента корзин
// зрелости (T-202/T-267).
type retryKey struct {
	zone int16
	id   int64
	day  time.Time
}

// retryUpdate выполняет пачечный UPDATE … RETURNING и возвращает ключи
// фактически переведённых доменов (после охран — как RETURNING cleanup,
// T-020: вторая параллельная транзакция, успевшая первой, не считается).
// Назначение повтора возвращает ключи с датой эпизода (третья колонка
// RETURNING — адресат декремента корзин, T-267); финализация — пару
// (зона, id). set — значение SET-колонки (check_state повтора или
// итоговый status), guardStatus/guardState — охрана WHERE.
func retryUpdate(ctx context.Context, tx pgx.Tx, sql, kind string,
	zoneIDs []int16, domainIDs []int64, set, guardStatus, guardState int16) ([]retryKey, error) {
	rows, err := tx.Query(ctx, sql, zoneIDs, domainIDs, set, guardStatus, guardState)
	if err != nil {
		// Результат при err != nil отбрасывается вызывателем (RetryOnce
		// возвращает RetryResult{}, err) — эквивалентных мутантов нет.
		return nil, fmt.Errorf("scheduler: повторные проверки temp_error: %s: %w", kind, err)
	}
	defer rows.Close()

	withDay := sql == retryRequeueSQL // ключи с датой эпизода — адресат декремента корзин (T-267)
	keys := make([]retryKey, 0, len(zoneIDs))
	for rows.Next() {
		var k retryKey
		var scanErr error
		if withDay {
			scanErr = rows.Scan(&k.zone, &k.id, &k.day)
		} else {
			scanErr = rows.Scan(&k.zone, &k.id)
		}
		if scanErr != nil {
			// Ключи при ошибке Scan не читаются вызывателем (см. выше),
			// ветка return закрыта тестом TestRetryOnceRowsErrors.
			return nil, fmt.Errorf("scheduler: повторные проверки temp_error: %s: чтение ключей: %w", kind, scanErr)
		}
		keys = append(keys, k)
	}
	if err := rows.Err(); err != nil {
		// Ключи при ошибке rows.Err() не читаются вызывателем (см. выше),
		// ветка return закрыта тестом TestRetryOnceRowsErrors.
		return nil, fmt.Errorf("scheduler: повторные проверки temp_error: %s: %w", kind, err)
	}
	return keys, nil
}

// RetryTempErrors — фоновая задача повторных проверок (ТЗ 5.3): по
// ticker выполняет дренаж всех созревших кандидатов пачками RetryOnce
// (первый дренаж — сразу при старте: накопленное за простой разбирается
// целиком, а не по пачке за тик), до отмены ctx и возвращает nil —
// контракт OnRun каркаса сервисов (T-012: чистое завершение по
// SIGTERM). Ошибка пачки прерывает только текущий дренаж: логируется
// (Warn), цикл живёт — очередной дренаж продолжит, проход идемпотентен
// (ТЗ 3.4).
func RetryTempErrors(ctx context.Context, pool RetryDB, opts RetryOptions) error {
	if pool == nil {
		return errors.New("scheduler: повторные проверки temp_error: пул не задан")
	}
	o := opts.withDefaults()
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log.Info("фоновые повторные проверки temp_error",
		"interval", o.Interval.String(), "batch", o.BatchSize,
		"retry_after", o.RetryAfter.String(), "max_retries", o.MaxRetries)

	// Дренаж до исчерпания кандидатов (стиль LeaseWatch/RecheckDaily):
	// один тик обрабатывает все созревшие пачки, а не одну — иначе
	// очередь повторов рассасывалась темпом BatchSize за интервал
	// (1000/час по дефолту: массовый DNS-сбой оставался бы в temp_error
	// месяцами). Пачки повторяются, пока проходят целиком; переведённые
	// в pending не выбираются повторно — дренаж конечен, переводы
	// подбирает главный цикл постановки (T-028).
	drain := func() {
		var total RetryResult
		for {
			res, err := RetryOnce(ctx, pool, o)
			switch {
			case err != nil && ctx.Err() != nil:
				// Отмена во время прохода — часть остановки, не тревога.
				return
			case err != nil:
				log.Warn("дренаж повторных проверок temp_error", "err", err,
					"requeued", total.Requeued, "finalized", total.Finalized)
				return
			}
			total.Requeued += res.Requeued
			total.Finalized += res.Finalized
			if res.Requeued+res.Finalized < o.BatchSize {
				if total.Requeued > 0 || total.Finalized > 0 {
					log.Info("дренаж повторных проверок temp_error",
						"requeued", total.Requeued, "finalized", total.Finalized)
				}
				return // неполная пачка — множество исчерпано
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

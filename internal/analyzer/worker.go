// Package analyzer — этап анализа Laravel (T-071; ТЗ 5.8 «Анализатор
// Laravel», §7 «analyzer → laravel_findings / laravel_domains», §13.1).
//
// Воркер потребляет q:analyze (сообщение Result — ключ строки
// subdomain_results: домен, субдомен, вариант; формат T-024; публикуют
// HTTP-этап T-058 после записи результата и render-worker T-067 после
// замены контента на итоговый DOM). Анализ пассивный (ТЗ 5.8): только уже
// скачанный контент, cookies и заголовки — новых запросов к сайтам нет.
// Обработка одной доставки:
//
//	SELECT строки (headers/cookies jsonb, content_hash — ТЗ 4.2: сообщение
//	несёт только идентификаторы, данные читаются из БД; после рендера
//	заголовки/cookies — данные итоговой навигации, T-067) → контент из
//	ContentStore по content_hash (нет хэша или контента — анализ по
//	заголовкам и кукам; HTML/DOM — материал целей html/meta, T-069)
//	→ Set.EvaluateCtx с DefaultLimits: снапшот набора правил берётся на
//	  каждую доставку (Reloader.Set; SIGHUP меняет набор без рестарта —
//	  подписка на смену не нужна, следующая доставка видит свежий)
//	→ ОДНА транзакция (ТЗ 3.4): UPDATE subdomain_results.rules_version —
//	  штамп «строка проанализирована набором версии N» (переанализ T-074
//	  находит записи с устаревшей версией; T-067 ставит NULL при смене
//	  контента) с охраной актуальности: content_hash IS NOT DISTINCT FROM
//	  прочитанного (строку перезаписал другой эпизод — перепроверка
//	  DELETE+INSERT T-058, рендер T-067), версия отлична от текущей
//	  (повторная доставка уже проштампованной строки — no-op, T-204) и
//	  домен не deleted (§8/§13.1); 0 строк — строка ушла из-под задачи:
//	  успех без записи (стиль renderworker), у нового эпизода свои
//	  публикация и анализ; RETURNING checked_at — декремент корзины
//	  хвостов reanalyze: (T-204) тем же шагом транзакции
//	→ запись laravel_findings по этому же ключу: ВСЕ срабатывания любого
//	  правила независимо от порога, is_laravel = score ≥ порога
//	  (§13.1/ТЗ 5.8); нет срабатываний — строка не пишется, существующая
//	  удаляется (§3.5: история не хранится, переанализ перезаписывает
//	  результат начисто)
//	→ XACK (Processing, T-025: подтверждение только после записи в БД).
//
// Идемпотентность повторной доставки (ТЗ 3.4): upsert по PK
// laravel_findings и повторный штамп rules_version тем же набором —
// состояние не меняется; сбой между шагами откатывает транзакцию целиком,
// повторная доставка проходит заново.
//
// Агрегация на уровне домена — T-072 (aggregate.go): той же транзакцией,
// сразу после findings, пересчитывается строка laravel_domains (§6.1:
// максимальный score, объединённый диапазон версий, found_on_mask,
// rules_version; ТЗ 5.8 «на уровне домена — агрегат»). Снятие находки
// переанализом (не-fresh) после фиксации запускает точечный дренаж
// контента домена (T-187, drainUnfound → content.DrainDomain): записи
// домена вернулись в кандидаты retention позади курсора дренажа (T-185).
package analyzer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"crawler/internal/content"
	"crawler/internal/db"
	"crawler/internal/metrics"
	"crawler/internal/model"
	"crawler/internal/queue"
	"crawler/internal/rules"
)

// SQL этапа. Коды схемы — литералами, а не данными (стиль record.go
// T-058); ключи строки — литералами типов. Ведущие колонки PK —
// партиционирование LIST по zone_id и субпартиционирование HASH (T-005)
// режут поиск до листа.
var (
	// analyzeSelectSQL — вход анализа: заголовки и cookies итогового
	// ответа (после рендера — итоговой навигации, T-067/T-058 — единый
	// jsonb-контракт) и ссылка на контент (HTML/DOM).
	analyzeSelectSQL = "SELECT headers, cookies, content_hash FROM subdomain_results " +
		"WHERE zone_id = $1::int2 AND domain_id = $2::int8 " +
		"AND subdomain_id = $3::int2 AND variant = $4::int2"

	// analyzeMarkSQL — штамп rules_version первой командой транзакции:
	// блокирует строку и решает судьбу записи (стиль recordDomainSQL
	// T-058). Охрана content_hash IS NOT DISTINCT FROM $6 —
	// оптимистическая (как renderUpdateSQL T-067): строка перезаписывается,
	// только пока её контент тот же, что прочитан (NULL сравнивается
	// корректно — «контента нет» легально, T-056). Охрана актуальности
	// версии rules_version IS DISTINCT FROM $5 (T-204): штамп ставится
	// только устаревшим строкам — NULL («никогда не анализировалась»)
	// и старой версии набора (дренаж смены правил T-074); уже
	// проштампованная текущей версией строка не затрагивается (no-op:
	// та же версия × тот же контент ⇒ детерминированно те же findings,
	// повторная доставка перезаписывала бы их байт-в-байт) — до T-204
	// повторный штамп выполнялся и давал ложный декремент корзине хвостов
	// reanalyze: (RETURNING ниже): у повторных доставок (републикации
	// тика, XAUTOCLAIM) нет парного инкремента записи. Охрана «домен не
	// deleted» — §8/§13.1: удалённому домену результат не записывается,
	// оставшиеся строки и findings вычистит фоновая очистка (T-020).
	// RETURNING r.checked_at (T-204) — час корзины хвостов для декремента
	// (analyzeReanalyzeConsumeSQL): час ЭПИЗОДА строки, как у инкрементов
	// записи/рендера; пустой результат (pgx.ErrNoRows) — охраны отсекли
	// (строки нет, контент сменился, домен удалён, версия уже актуальна):
	// успех без записи (стиль renderworker).
	analyzeMarkSQL = "UPDATE subdomain_results AS r SET rules_version = $5::int4 " +
		"WHERE r.zone_id = $1::int2 AND r.domain_id = $2::int8 " +
		"AND r.subdomain_id = $3::int2 AND r.variant = $4::int2 " +
		"AND r.content_hash IS NOT DISTINCT FROM $6::bytea " +
		"AND r.rules_version IS DISTINCT FROM $5::int4 " +
		fmt.Sprintf("AND NOT EXISTS (SELECT 1 FROM domains AS d WHERE d.zone_id = r.zone_id "+
			"AND d.id = r.domain_id AND d.status = %d)", int16(model.StatusDeleted)) +
		" RETURNING r.checked_at"

	// analyzeReanalyzeConsumeSQL — декремент корзины неанализированных
	// хвостов crawler_counters['reanalyze:YYYY-MM-DD"T"HH24'] (T-204) в
	// транзакции штампа: −1 на переведённую устаревшей→текущей строку;
	// $1 — checked_at строки из RETURNING analyzeMarkSQL, час корзины
	// вычисляет БД (date_trunc) — тот же ключ, что у инкрементов записи
	// результата (record.go, час now() = checked_at строк) и замены
	// контента рендером (renderworker, час эпизода из RETURNING). Guard
	// тикового прохода (reanalyzeGuardSQL) суммирует корзины старше
	// текущего часа: опубликованный и проштампованный хвост возвращает
	// корзине ноль, следующий тик не вычитывает окно. Штампы строк СТАРОЙ
	// версии (дренаж смены правил, републикации тика) декрементируют без
	// парного инкремента — отрицательный дрейф до прохода реконсиляции
	// (FILTER-позиции prev/cur агрегата окна, monitor): держит guard
	// закрытым на время дренажа — у тика там своя работа пересекается
	// только дублями, семантика потоковая (семейство T-202/T-203).
	// Upsert-паттерн record.go; порядок блокировок — единственная строка
	// счётчика транзакции, после зон/скаляров агрегации: общий с проходом
	// реконсиляции (reanalyze: после retry_mature:, перед rendered:) —
	// без циклов.
	analyzeReanalyzeConsumeSQL = "INSERT INTO crawler_counters (name, value, updated_at) " +
		"VALUES ('reanalyze:' || to_char(date_trunc('hour', $1::timestamptz), 'YYYY-MM-DD\"T\"HH24'), $2::int8, now()) " +
		"ON CONFLICT (name) DO UPDATE " +
		"SET value = crawler_counters.value + EXCLUDED.value, updated_at = now()"

	// findingUpsertSQL — запись срабатываний: все правила независимо от
	// порога (§13.1), is_laravel отделяет уверенные срабатывания от слабых.
	// Upsert по PK: повторная доставка и переанализ перезаписывают строку
	// (§3.5 — только последний результат). ver_min/ver_max NULL при
	// отсутствии границы и при конфликте диапазонов (T-070: неопределённый
	// диапазон — обе границы пустые); signals — jsonb-текст, приводится на
	// сервере (стиль record.go: bytea в jsonb PostgreSQL не приводит).
	findingUpsertSQL = "INSERT INTO laravel_findings (" +
		"zone_id, domain_id, subdomain_id, variant, " +
		"score, ver_min, ver_max, signals, is_laravel, rules_version) " +
		"VALUES ($1::int2, $2::int8, $3::int2, $4::int2, " +
		"$5::float4, $6::text, $7::text, $8::jsonb, $9::bool, $10::int4) " +
		"ON CONFLICT (zone_id, domain_id, subdomain_id, variant) DO UPDATE SET " +
		"score = $5::float4, ver_min = $6::text, ver_max = $7::text, " +
		"signals = $8::jsonb, is_laravel = $9::bool, rules_version = $10::int4"

	// findingDeleteSQL — ни одно правило не сработало: строка не пишется,
	// существующая (прошлый анализ находил признаки) удаляется — §3.5
	// перезапись: результат текущего эпизода «признаков нет».
	findingDeleteSQL = "DELETE FROM laravel_findings " +
		"WHERE zone_id = $1::int2 AND domain_id = $2::int8 " +
		"AND subdomain_id = $3::int2 AND variant = $4::int2"
)

// AnalyzerDB — пул для чтения строки результата и записи findings:
// db.DB с транзакциями (стиль httpworker.RecordDB, renderworker.RenderDB).
// Штамп и findings идут одной транзакцией (ТЗ 3.4); *pgxpool.Pool
// удовлетворяет интерфейсу.
type AnalyzerDB interface {
	db.DB
	Begin(ctx context.Context) (pgx.Tx, error)
}

// ContentGetter — шов хранилища контента (ТЗ 5.7): чтение HTML/DOM для
// матчинга. Узкое подмножество контракта content.ContentStore (этапу
// нужен только Get); удовлетворяется *content.PackStore (T-042).
type ContentGetter interface {
	Get(ctx context.Context, hash content.Hash) ([]byte, error)
}

// SetSource — текущий снапшот набора правил (ТЗ 5.8: перезагрузка по
// SIGHUP без перезапуска). Удовлетворяется *rules.Reloader (T-068);
// каждая доставка берёт актуальный набор — подписка на смену не нужна.
// nil-снапшот (у Reloader не бывает) — доставка пропускается с Warn,
// строка остаётся без штампа и вернётся переанализом (T-074).
type SetSource interface {
	Set() *rules.Set
}

// Дефолты воркера.
const (
	// DefaultGroup — группа потребителей q:analyze (ТЗ 4.2: consumer
	// groups масштабируют воркеров горизонтально).
	DefaultGroup = "analyzer"

	// DefaultConcurrency — доставок q:analyze в работе одновременно.
	// Анализ лёгок (RE2-матчинг по ограниченному входу — микросекунды,
	// T-069; пара операторов БД), окно держит конвейер непрерывным, не
	// создавая очереди к пулу соединений; масштабирование — инстансами
	// (ТЗ 4.1), потолок поднимается Options.Concurrency.
	DefaultConcurrency = 8

	// DefaultConsume — записей q:analyze за одно чтение: окно чтения не
	// глубже запаса обработки (стиль httpworker/renderworker).
	DefaultConsume = 16

	// DefaultErrorWait — пауза после ошибок Redis и БД (стиль
	// httpworker.DefaultErrorWait): холостые круги не греют логи,
	// неподтверждённые доставки вернёт XAUTOCLAIM (T-025).
	DefaultErrorWait = 30 * time.Second
)

// Options — параметры воркера; нулевые значения заменяются дефолтами
// (withDefaults, стиль renderworker.Options).
type Options struct {
	// Concurrency — доставок q:analyze в работе одновременно; 0 —
	// DefaultConcurrency.
	Concurrency int
	// Consume — записей q:analyze за одно чтение; 0 — DefaultConsume.
	Consume int
	// Group — группа потребителей q:analyze; "" — DefaultGroup.
	Group string
	// Consumer — имя потребителя группы; "" — "analyzer-<pid>".
	Consumer string
	// MinScore — порог is_laravel (analyzer.laravel_min_score, ТЗ 5.8/
	// §13.1: score ≥ 0.6). Неположительное значение заменяется
	// DefaultMinScore внутри Result.IsLaravel (T-070) — валидация конфига
	// допускает только (0, 1], защита для прямых вызовов.
	MinScore float64
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
	// Retention — окно жизни контента не-находок (content.retention,
	// T-166): им точечный дренаж DrainDomain чистит контент домена при
	// снятии находки переанализом (T-187). ≤ 0 — retention выключен
	// (контент хранится всегда) — дренажа нет, событие только считается.
	Retention time.Duration
	// DrainDomain — шов точечного дренажа контента домена (T-187):
	// удовлетворяется content.DrainDomain (пул воркера —
	// content.RetentionDB); nil — дренаж выключен (изолированные сборки,
	// юнит-тесты без контентной семантики) — событие только считается.
	DrainDomain func(ctx context.Context, pool content.RetentionDB,
		retention time.Duration, zoneID int16, domainID int64) (content.RetentionStats, error)
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
		o.Consumer = fmt.Sprintf("analyzer-%d", os.Getpid())
	}
	return o
}

func (o Options) log() *slog.Logger {
	if o.Log == nil {
		return slog.New(slog.DiscardHandler)
	}
	return o.Log
}

// Worker — анализатор Laravel: обрабатывает задачи q:analyze (ключи строк
// subdomain_results) через швы БД, хранилища контента и набора правил.
// Потокобезопасен: состояние неизменно после New.
type Worker struct {
	db       AnalyzerDB
	q        queue.Queue
	contents ContentGetter
	sets     SetSource
	opt      Options
	met      *Metrics // приборы этапа (T-077); nil — no-op
}

// New строит воркер поверх пула БД, очереди, хранилища контента и
// источника набора правил. nil-зависимости — ошибка: тихая потеря
// анализа незаметна для оператора (стиль T-058/renderworker).
func New(db AnalyzerDB, q queue.Queue, contents ContentGetter, sets SetSource, opts Options) (*Worker, error) {
	if db == nil {
		return nil, errors.New("analyzer: пул БД не задан")
	}
	if q == nil {
		return nil, errors.New("analyzer: очередь не задана")
	}
	if contents == nil {
		return nil, errors.New("analyzer: хранилище контента не задано")
	}
	if sets == nil {
		return nil, errors.New("analyzer: источник набора правил не задан")
	}
	return &Worker{db: db, q: q, contents: contents, sets: sets, opt: opts.withDefaults(), met: NewMetrics(opts.Metrics)}, nil
}

// analyzeRow — прочитанная строка subdomain_results: вход пассивного
// анализа (ТЗ 5.8) — заголовки, cookies и контент.
type analyzeRow struct {
	headers map[string][]string
	cookies []rules.Cookie
	hash    []byte // nil — контента нет (NULL content_hash, T-056)
}

// ProcessResult обрабатывает одну задачу q:analyze (§5.8, §7): пассивный
// анализ строки по текущему набору правил, запись laravel_findings и
// пересчёт агрегата домена laravel_domains (T-072) в одной транзакции со
// штампом rules_version. Возвращает true, когда транзакция зафиксирована —
// findings записаны (все срабатывания независимо от порога) и агрегат
// домена пересчитан, либо при пустом итоге удалена прошлая строка и
// поставлен штамп (§3.5: «признаков нет» — тоже результат анализа).
// false без ошибки — задачи делать нечего: строки нет (домен удалён/
// перепроверен без этого ключа) или она ушла из-под задачи (контент
// сменился после чтения — охрана UPDATE; рендер/перепроверка опубликуют
// свой q:analyze) — это успех, ничего не пишется, доставка подтверждается.
// Ошибка — сбой любого шага: доставка не подтверждается и вернётся
// повторной доставкой (Processing, T-025); повторная обработка
// идемпотентна (upsert).
func (w *Worker) ProcessResult(ctx context.Context, task queue.Result) (bool, error) {
	row, found, err := w.selectResult(ctx, task)
	if err != nil {
		return false, err
	}
	if !found {
		w.opt.log().Debug("анализ не требуется — строки нет",
			"zone_id", task.ZoneID, "domain_id", task.DomainID,
			"subdomain_id", task.SubdomainID, "variant", int16(task.Variant))
		return false, nil
	}
	name := analyzeLogName(task)

	html, err := w.fetchContent(ctx, row.hash)
	if err != nil {
		return false, fmt.Errorf("analyzer: %s: %w", name, err)
	}

	// Снапшот набора на доставку: Reloader.Set() атомарен, EvaluateCtx
	// работает с иммутабельным Set; смена набора посреди страницы
	// невозможна, следующая доставка возьмёт свежий.
	set := w.sets.Set()
	if set == nil {
		// Симметрично ReanalyzeLoop: nil-снапшот альтернативного SetSource —
		// пропуск, не паника. Доставка подтверждается: без штампа версии
		// строка остаётся устаревшей, переанализ (T-074) к ней вернётся.
		w.opt.log().Warn("набор правил не загружен — анализ пропущен, строка останется для переанализа",
			"domain", name, "zone_id", task.ZoneID, "domain_id", task.DomainID)
		return false, nil
	}
	res := set.EvaluateCtx(ctx, rules.Page{
		HTML:    html,
		Cookies: row.cookies,
		Headers: row.headers,
	}, rules.Limits{}) // нулевые Limits → DefaultLimits (T-069)
	if res.Partial {
		// Бюджет времени исчерпан (EvalTimeout) или ctx отменён — правила
		// просмотрены не все: усечённый результат нельзя записывать и
		// штамповать rules_version как полный. Доставка подтверждается:
		// строка остаётся устаревшей, переанализ (T-074) вернётся к ней
		// (практически недостижимо при RE2 на ограниченном входе).
		w.opt.log().Warn("анализ прерван до полного просмотра правил — findings не записаны, строка останется для переанализа",
			"domain", name, "signals", len(res.Signals), "rules_version", set.Version)
		return false, nil
	}
	isLaravel := res.IsLaravel(w.opt.MinScore)

	signals, err := marshalSignals(res.Signals)
	if err != nil {
		return false, fmt.Errorf("analyzer: %s: %w", name, err)
	}

	tx, err := w.db.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("analyzer: %s: запись findings: транзакция: %w", name, err)
	}
	// Откат безусловно: после Commit — no-op (стиль T-044/T-058).
	defer func() { _ = tx.Rollback(ctx) }()

	// Штамп первым шагом: охраны актуальности решают, писать ли findings
	// (нет строки — строка ушла из-под задачи, домен удалён, контент
	// сменился или версия уже текущая — повторная доставка, T-204).
	// RETURNING checked_at — час корзины хвостов для декремента ниже.
	var stampedAt time.Time
	err = tx.QueryRow(ctx, analyzeMarkSQL,
		task.ZoneID, task.DomainID, task.SubdomainID, int16(task.Variant),
		set.Version, row.hash).Scan(&stampedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			w.opt.log().Debug("findings не записаны — строка перезаписана другим эпизодом, домен удалён или уже проанализирована этой версией",
				"domain", name, "zone_id", task.ZoneID, "domain_id", task.DomainID,
				"subdomain_id", task.SubdomainID, "variant", int16(task.Variant))
			return false, nil
		}
		return false, fmt.Errorf("analyzer: %s: штамп rules_version: %w", name, err)
	}

	if len(res.Signals) > 0 {
		// Все срабатывания независимо от порога (§13.1); повторная
		// доставка и переанализ перезаписывают (ON CONFLICT, §3.5).
		tag, err := tx.Exec(ctx, findingUpsertSQL,
			task.ZoneID, task.DomainID, task.SubdomainID, int16(task.Variant),
			float32(res.Score), nullStr(res.VerMin), nullStr(res.VerMax),
			signals, isLaravel, set.Version)
		if err != nil {
			return false, fmt.Errorf("analyzer: %s: запись findings: %w", name, err)
		}
		if tag.RowsAffected() != 1 {
			return false, fmt.Errorf("analyzer: %s: запись findings: затронуто %d строк", name, tag.RowsAffected())
		}
	} else {
		// Ни одно правило не сработало — строка не пишется (ТЗ 5.8);
		// существующая удаляется: прошлый анализ видел признаки, которых
		// в текущем эпизоде нет (§3.5).
		if _, err := tx.Exec(ctx, findingDeleteSQL,
			task.ZoneID, task.DomainID, task.SubdomainID, int16(task.Variant)); err != nil {
			return false, fmt.Errorf("analyzer: %s: удаление findings: %w", name, err)
		}
	}

	// Агрегация домена — T-072: пересчёт laravel_domains по findings
	// домена той же транзакцией (решения и согласование параллельных
	// доставок — aggregate.go); fresh — признак свежего эпизода HTTP
	// в задаче: при переходе пишет историю appeared/lost (T-118);
	// unfound — сигнал снятия находки переанализом (T-187).
	unfound, err := aggregateDomain(ctx, tx, task.ZoneID, task.DomainID, set.Version, task.Fresh)
	if err != nil {
		return false, fmt.Errorf("analyzer: %s: %w", name, err)
	}

	// Декремент корзины хвостов reanalyze: (T-204) — последним шагом
	// транзакции (хвостовая дельта, стиль record.go): строка переведена
	// устаревшей→текущей — опубликованный хвост возвращает корзине ноль,
	// guard следующего тика закрывается. Откат откатывает штамп и дельту
	// вместе (§3.4).
	if err := analyzeConsumeTail(ctx, tx, stampedAt); err != nil {
		return false, fmt.Errorf("analyzer: %s: %w", name, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("analyzer: %s: запись findings: фиксация: %w", name, err)
	}

	// Снятие находки переанализом (T-187) — после фиксации: событие уже
	// состоялось, ошибки дренажа не роняют доставку (findings и агрегат
	// зафиксированы, XACK состоится; записи домена без дренажа остались
	// бы позади курсора retention — класс до T-187, см. drainUnfound).
	w.drainUnfound(ctx, task, name, unfound)

	w.opt.log().Debug("анализ записан",
		"domain", name, "signals", len(res.Signals), "score", res.Score,
		"is_laravel", isLaravel, "ver_min", res.VerMin, "ver_max", res.VerMax,
		"conflict", res.Conflict, "rules_version", set.Version)
	return true, nil
}

// analyzeConsumeTail — декремент корзины неанализированных хвостов
// reanalyze: в ОТКРЫТОЙ транзакции штампа (T-204): stampedAt — час
// корзины (RETURNING analyzeMarkSQL), дельта — −1 (одна строка
// переведена устаревшей→текущей). Upsert сам создаёт корзину часа
// (декремент теоретически может опередить инкремент — дубль-доставка
// после потери строки эпизода) — RowsAffected гарантированно 1,
// проверка — страховка прямых вызовов с чужим SQL (молчаливая потеря
// дельты держала бы guard тика открытым до реконсиляции). Возврат с
// ошибкой откатывает транзакцию вызывающего — штамп и корзина меняются
// только вместе (§3.4).
func analyzeConsumeTail(ctx context.Context, tx pgx.Tx, stampedAt time.Time) error {
	tag, err := tx.Exec(ctx, analyzeReanalyzeConsumeSQL, stampedAt, int64(-1))
	if err != nil {
		return fmt.Errorf("корзина хвостов переанализа: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("корзина хвостов переанализа: затронуто %d строк (upsert корзины часа обязан 1)",
			tag.RowsAffected())
	}
	return nil
}

// drainUnfound — точечный дренаж контента домена при снятии находки
// переанализом (T-187): сигнал unfound агрегации (aggregateDomain,
// переход строки laravel_domains доставкой без fresh) означает, что
// старые записи домена subdomain_results вернулись в множество
// кандидатов retention позади курсора дренажа (T-185) — часовой проход
// их больше не увидит. Дренаж — content.DrainDomain: те же фильтры
// отбора и purge-механика T-166 по префиксу PK (zone_id, domain_id).
// Вызывается после фиксации транзакции доставки: сбой дренажа — Warn и
// метрика, доставка подтверждается (анализ — ядро работы; записи без
// дренажа остаются в классе до T-187, их подберут редкие catch-up-круги
// T-185 либо перепроверка T-030). Идемпотентен: повтор не найдёт
// кандидатов. Отмена ctx — часть остановки, не тревога.
func (w *Worker) drainUnfound(ctx context.Context, task queue.Result, name string, unfound bool) {
	if !unfound {
		return
	}
	w.met.Unfound()
	if w.opt.DrainDomain == nil || w.opt.Retention <= 0 {
		// Минимум T-187 при выключенном дренаже: retention = 0 — контент
		// хранится всегда (цикла retention нет вовсе, позади курсора ничего
		// не копится); шов не задан — изолированная сборка. Событие
		// наблюдаемо метрикой и логом.
		w.opt.log().Info("снятие находки переанализом — точечный дренаж контента выключен",
			"domain", name, "zone_id", task.ZoneID, "domain_id", task.DomainID)
		return
	}
	stats, err := w.opt.DrainDomain(ctx, w.db, w.opt.Retention, task.ZoneID, task.DomainID)
	if err != nil {
		if ctx.Err() == nil {
			w.opt.log().Warn("снятие находки переанализом: точечный дренаж контента домена не выполнен — записи останутся позади курсора retention",
				"domain", name, "zone_id", task.ZoneID, "domain_id", task.DomainID, "err", err)
		}
		w.met.UnfoundDrainError()
		return
	}
	w.met.UnfoundDrain(stats.Rows)
	if stats.Rows > 0 {
		w.opt.log().Info("снятие находки переанализом — контент домена дренирован точечно",
			"domain", name, "zone_id", task.ZoneID, "domain_id", task.DomainID,
			"rows", stats.Rows, "refs", stats.Refs, "contents", stats.Contents,
			"freed_bytes", stats.FreedBytes)
	}
}

// selectResult читает вход анализа по ключу задачи: заголовки, cookies и
// ссылку на контент. SELECT вне транзакции: конкурентная перезапись
// строки между чтением и UPDATE ловится охраной analyzeMarkSQL (стиль
// renderworker.selectTarget).
func (w *Worker) selectResult(ctx context.Context, task queue.Result) (analyzeRow, bool, error) {
	rows, err := w.db.Query(ctx, analyzeSelectSQL,
		task.ZoneID, task.DomainID, task.SubdomainID, int16(task.Variant))
	if err != nil {
		return analyzeRow{}, false, fmt.Errorf("analyzer: чтение строки результата: %w", err)
	}
	defer rows.Close()

	if rows.Next() {
		var headers, cookies *string
		var hash []byte
		if err := rows.Scan(&headers, &cookies, &hash); err != nil {
			return analyzeRow{}, false, fmt.Errorf("analyzer: чтение строки результата: %w", err)
		}
		row := analyzeRow{hash: hash}
		if row.headers, err = parseHeadersJSON(headers); err != nil {
			return analyzeRow{}, false, fmt.Errorf("analyzer: заголовки строки результата: %w", err)
		}
		if row.cookies, err = parseCookiesJSON(cookies); err != nil {
			return analyzeRow{}, false, fmt.Errorf("analyzer: cookies строки результата: %w", err)
		}
		return row, true, nil // PK — не больше одной строки
	}
	if err := rows.Err(); err != nil {
		return analyzeRow{}, false, fmt.Errorf("analyzer: чтение строки результата: %w", err)
	}
	return analyzeRow{}, false, nil
}

// fetchContent достаёт контент для матчинга целей html/meta (T-069):
// NULL/урезанный content_hash и отсутствующий контент (ссылки сняты) —
// контента нет, анализ продолжается по заголовкам и кукам (ТЗ 5.8:
// правила по шести целям, html/meta — часть набора, не весь анализ).
// Пустой DOM — тот же исход: движок по пустому входу не найдёт html/
// meta-признаков, куки и заголовки продолжают работать.
func (w *Worker) fetchContent(ctx context.Context, hash []byte) ([]byte, error) {
	if len(hash) != content.HashLen {
		return nil, nil
	}
	var h content.Hash
	copy(h[:], hash)
	data, err := w.contents.Get(ctx, h)
	if errors.Is(err, content.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("чтение контента %s: %w", h.String(), err)
	}
	return data, nil
}

// analyzeLogName — имя для логов: имя домена из сообщения, иначе ключ
// строки (Name опционально, T-024).
func analyzeLogName(task queue.Result) string {
	if task.Name != "" {
		return task.Name
	}
	return fmt.Sprintf("%d/%d/%d/%d", task.ZoneID, task.DomainID, task.SubdomainID, int16(task.Variant))
}

// nullStr — пустая граница диапазона → NULL (отсутствует или конфликт,
// T-070).
func nullStr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// signalJSON — свидетельство в формате колонки laravel_findings.signals
// (миграция 000001: [{rule, weight, evidence}, …]). Теги нижним
// регистром по эскизу схемы §6.1; rules.Evidence тегов не имеет — формат
// jsonb вводит этап записи.
type signalJSON struct {
	Rule     string  `json:"rule"`
	Weight   float64 `json:"weight"`
	Evidence string  `json:"evidence"`
}

// marshalSignals сериализует срабатывания; пустой набор — пустая строка
// (вызов не делается: строка findings не пишется вовсе).
func marshalSignals(sigs []rules.Evidence) (string, error) {
	if len(sigs) == 0 {
		return "", nil
	}
	out := make([]signalJSON, 0, len(sigs))
	for _, s := range sigs {
		out = append(out, signalJSON{Rule: s.Rule, Weight: s.Weight, Evidence: s.Evidence})
	}
	b, err := json.Marshal(out)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// parseHeadersJSON разбирает jsonb subdomain_results.headers: контракт
// T-058 (http.Header — канонические имена MIME, значения массивы) и T-067
// (заголовки итоговой навигации рендера). Разбор в map[string][]string,
// а не http.Header: канонизация уже выполнена пишущим этапом, а
// rules.Page.Headers ждёт именно map (T-069).
func parseHeadersJSON(s *string) (map[string][]string, error) {
	if s == nil || *s == "" {
		return nil, nil
	}
	var h map[string][]string
	if err := json.Unmarshal([]byte(*s), &h); err != nil {
		return nil, err
	}
	return h, nil
}

// cookieJSON — запись jsonb subdomain_results.cookies: контракт T-058
// (recordCookie) и T-067 (render.ExtractCookie) — {name, value} плюс
// атрибуты; движку правил нужны имя и значение (ТЗ 5.8), атрибуты
// игнорируются (неизвестные поля JSON пропускаются).
type cookieJSON struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

func parseCookiesJSON(s *string) ([]rules.Cookie, error) {
	if s == nil || *s == "" {
		return nil, nil
	}
	var raw []cookieJSON
	if err := json.Unmarshal([]byte(*s), &raw); err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, nil
	}
	out := make([]rules.Cookie, 0, len(raw))
	for _, c := range raw {
		out = append(out, rules.Cookie{Name: c.Name, Value: c.Value})
	}
	return out, nil
}

// --- рабочий цикл ------------------------------------------------------------

// Run — рабочий цикл analyzer (ТЗ 4.2, §5.8, §7): читает задачи q:analyze
// группой потребителей окном Consume и обрабатывает доставки параллельно
// до Concurrency, каждую — паттерном Processing (T-025): анализ → запись
// findings и штампа rules_version в БД → XACK. Работает до отмены ctx и
// возвращает nil — контракт OnRun каркаса (T-012; начатые доставки
// дорабатывают или возвращаются очереди повторной доставкой, ТЗ 3.4).
// Ошибка чтения или обработки не останавливает цикл: логируется (Warn),
// неподтверждённая доставка вернётся XAUTOCLAIM (ТЗ 4.2).
func Run(ctx context.Context, w *Worker, opts Options) error {
	if w == nil {
		return errors.New("analyzer: цикл воркера: воркер не задан")
	}
	o := opts.withDefaults()
	log := o.log()
	log.Info("цикл analyzer",
		"group", o.Group, "consumer", o.Consumer,
		"concurrency", o.Concurrency, "consume", o.Consume,
		"min_score", w.opt.MinScore)

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
	// Живёт до отмены ctx; Run дожидается его выхода (начатая задача
	// дорабатывает или остаётся неподтверждённой, ТЗ 3.4).
	if o.Recover != nil {
		recDone := make(chan struct{})
		go func() {
			defer close(recDone)
			_ = queue.RecoverLoop(ctx, o.Recover, w.processDelivery, queue.RecoverOptions{
				Stream:   queue.StreamAnalyze,
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
	for {
		deliveries, err := w.q.Consume(ctx, queue.StreamAnalyze, o.Group, o.Consumer, o.Consume)
		if err != nil {
			if ctx.Err() != nil {
				return nil // graceful shutdown
			}
			log.Warn("цикл analyzer: чтение q:analyze", "err", err)
			wait(DefaultErrorWait)
			continue
		}

		var wg sync.WaitGroup
		for _, d := range deliveries {
			// Эквивалентный мутант (T-M10): вырезание break запускает
			// оставшиеся доставки окна и после отмены ctx, но их обработка
			// целиком падает на отменённом контексте (SELECT/транзакция/XACK
			// отклоняют ctx и у драйверов, и у фейков) без записей в БД и без
			// подтверждения — запись остаётся в pending, как и при невызове;
			// Warn погашен условием ctx.Err() == nil ниже.
			// mutator-disable-next-line branch/if
			if ctx.Err() != nil {
				// Эквивалентный мутант (T-M10): break → continue проходит
				// остаток окна, не запуская доставки, и выходит на
				// пост-проверку ctx за wg.Wait — то же состояние и возврат nil.
				// mutator-disable-next-line loop/break
				break // не начатые доставки окна не подтверждены
			}
			sem <- struct{}{}
			wg.Add(1)
			go func(d queue.Delivery) {
				defer wg.Done()
				defer func() { <-sem }()
				err := queue.Processing(ctx, w.q, o.Group, d, w.processDelivery)
				if err != nil && ctx.Err() == nil {
					log.Warn("задача q:analyze не подтверждена — вернётся повторной доставкой",
						"entry", d.ID, "err", err)
				}
			}(d)
		}
		wg.Wait()
		if ctx.Err() != nil {
			return nil // остановка: окно доработано или возвращено очереди
		}
	}
}

// processDelivery обрабатывает одну доставку q:analyze: декодирование
// Result-ключа и анализ. Подтверждение (XACK) выполняет Processing после
// nil-возврата — «XACK только после записи результата в БД» (ТЗ 4.2);
// ядовитая запись (вне формата T-024) возвращается ошибкой — очередь
// исчерпает попытки и унесёт её в q:dead (T-025). Успешная обработка
// идёт в приборы этапа (T-077): счётчик задач и длительность.
func (w *Worker) processDelivery(ctx context.Context, del queue.Delivery) error {
	task, err := queue.Decode[queue.Result](del)
	if err != nil {
		return fmt.Errorf("analyzer: запись %s стрима %s: %w", del.ID, del.Stream, err)
	}
	start := time.Now()
	if _, err := w.ProcessResult(ctx, task); err != nil {
		return fmt.Errorf("analyzer: домен %s: анализ: %w", task.Name, err)
	}
	w.met.Task(time.Since(start))
	return nil
}

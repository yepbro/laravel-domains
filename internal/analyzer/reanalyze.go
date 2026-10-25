// Package analyzer — анализ сохранённого контента по правилам Laravel
// (ТЗ 5.8): пассивный, по уже скачанным страницам, cookies и заголовкам.
// Потребление q:analyze и запись laravel_findings — T-071 (worker.go).
//
// Этот файл — переанализ (T-074, ТЗ 5.8 «Переанализ»: «фоновая задача
// находит записи с устаревшей версией правил и ставит их в q:analyze;
// повторное скачивание не требуется»; приёмка §11.4 п. 4 — «изменение
// правил + reanalyze обновляет результаты без повторного скачивания»).
//
// Критерий устаревания — rules_version IS DISTINCT FROM текущей версии
// набора (rules.Set.Version, T-068), а не «<>»: NULL («никогда не
// анализировалась» — строки, записанные до T-071, и новые эпизоды
// проверки T-058) выпадали бы из сравнения NULL <> x = NULL. Охрана
// §13.1 — deleted-домены вне цикла (status <> deleted, стиль выборок
// scheduler T-030; их результаты чистит T-020). Отключённые зоны не
// фильтруются: §5.10/§4.1 выключение зоны останавливает новые проверки,
// переанализ уже сохранённых результатов сеть не трогает и правил не
// нарушает.
//
// Повторное скачивание не выполняется: сообщение Result (T-024) несёт
// только ключ строки (домен, субдомен, вариант) и имя для логов; воркер
// T-071 берёт контент из хранилища по ключу.
//
// Дренаж пачками с курсорной пагинацией — ДВЕ формы (T-193, диагноз
// записи №12 журнала медленных 25.10: часовой тик с нулевым PK-курсором
// гнал Parallel Seq Scan всей subdomain_results — 4,8 млн строк, 19,9 с —
// ради горстки свежих хвостов). Публикация q:analyze строку не меняет:
// rules_version зафиксирует воркер, асинхронно; повторная выборка «с
// начала» брала бы ту же пачку in-flight строк и дублировала её с каждой
// итерацией. Поэтому:
//
//   - PK-форма (reanalyzeSelectSQL) — ПОЛНЫЙ дренаж устаревшего множества
//     (ReanalyzeDrain): смена правил (Subscribe, T-068) и crawlerctl
//     reanalyze; редкий, обязан пройти всё множество с нуля, seq scan по
//     неиндексируемому rules_version (HOT-инвариант, класс T-104/T-175)
//     осознанно полон;
//   - время-форма (reanalyzeTickSQL) — часовой тик: множество кандидатов
//     тика — свежий срез (новые эпизоды T-058 со свежим checked_at и
//     NULL-версией; хвосты, чья публикация отстала/потерялась),
//     композитный курсор (checked_at, PK) опирается на индекс
//     subdomain_results_checked_idx (T-171) — проход читает только окно
//     вперёд курсора, стоимость ∝ притоку, а не размеру таблицы
//     (прецедент — курсор retention T-185).
//
// Дубликаты между проходами ограничены (in-flight окно) и гасятся
// идемпотентностью потребителя по ключу — как у T-066.
//
// Backpressure — WatermarkPublisher (T-026): публикация ждёт ёмкости
// q:analyze (единый порог scheduler.queue_high_watermark), отмена ctx —
// чистый выход. Опубликованное до сбоя не откатывается (at-least-once,
// §3.4): повторный проход опубликует снова — дубликат ограничен.
//
// Контракт с воркером T-071: по каждому обработанному Result воркер
// фиксирует subdomain_results.rules_version = версия набора — в том
// числе строкам без сохранённого контента («анализировать нечего» —
// тоже результат с версией), иначе фоновый проход возвращался бы к ним
// бесконечно.
package analyzer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"crawler/internal/db"
	"crawler/internal/model"
	"crawler/internal/queue"
	"crawler/internal/rules"
)

// Дефолты прохода (стиль T-020/T-030/T-066: константы, отдельной опции
// конфига нет — ТЗ 5.10 состав фиксирован; рабочие значения цикл
// получает опциями для тестов и вызова из CLI).
const (
	// DefaultReanalyzeInterval — период тикового прохода. Смена правил
	// обрабатывается немедленно — по уведомлению Reloader (Subscribe,
	// T-068); тикер — страховка: потерянные сообщения (усечение
	// MAXLEN ~, T-025), сбой воркера, строки новых эпизодов проверки.
	// Час, а не минута (стиль T-020/T-030/T-066): тиковый проход —
	// время-форма по свежему срезу (T-193), но каждый проход всё же
	// перечитывает окно вперёд курсора и перепубликует ещё не
	// проштампованные строки новых эпизодов (дубликаты гасятся
	// идемпотентностью воркера) — учащать незачем; и не сутки (стиль
	// T-030): страховка не должна ждать перепроверочного интервала.
	DefaultReanalyzeInterval = time.Hour

	// DefaultReanalyzeBatch — строк в одной пачке: проход читает только
	// ключи (контент берёт воркер по ключу), 5000 совпадает с пачками
	// планировщика (DefaultRecheckBatch, T-030) — короткая выборка и
	// ограниченная серия публикаций до проверки ёмкости гейтом.
	DefaultReanalyzeBatch = 5000

	// DefaultReanalyzeLookback — окно якоря курсора тикового прохода
	// (T-193 §4.2): курсор тика стартует с now() − lookback, а не с
	// эпохи (полный дренаж старта цикла покрывает всё устаревшее на
	// момент запуска — тику остаются только свежие хвосты). 24 ч:
	// страховка тика обслуживает СВЕЖИЕ потери (основной путь —
	// немедленная публикация record.go и дренаж по Subscribe), всё
	// старше либо проанализировано (rules_version меняется только
	// записью анализа воркером T-071 или сменой правил; новый эпизод
	// даёт свежий checked_at, T-058), либо это остаточный класс —
	// потерянное старше окна без смены правил, территория следующего
	// полного дренажа и наблюдаемости q:dead/q:analyze (T-162;
	// docs/analyzer.md). Окно переживает тиковый интервал с запасом
	// (простой цикла до суток между тиками/рестартами) и суточный ритм
	// смен правил; дороже оно — только первому проходу после потери
	// курсора (одно окно ∝ притоку, не размеру таблицы). Якорь считается
	// по часам процесса (Go now()): расхождение часов приложения и БД
	// на фоне окна 24 ч несущественно, дальше курсор ведут сами
	// значения checked_at (их пишет now() сервера БД, T-058).
	// Персистентность курсора в БД отклонена сознательно (ТЗ T-193
	// §6.5): якорь делает потерю курсора дешёвой.
	DefaultReanalyzeLookback = 24 * time.Hour
)

// reanalyzeSelectSQL — пачка устаревших строк PK-ФОРМЫ — ПОЛНЫЙ ДРЕНАЖ
// (ReanalyzeDrain: Subscribe при смене правил T-068, crawlerctl
// reanalyze; разовая пачка — ReanalyzeOnce): версия отлична от текущей
// (IS DISTINCT FROM: NULL — никогда не анализировалась), домен не
// удалён (§13.1), зона — опционально (NULL параметр — все). Курсор
// row-value > по ключу строки (PK) отсекает уже пройденное дренажем;
// ORDER BY по тому же ключу — детерминированные пачки и корректная
// пагинация. Дренаж редок и обязан пройти всё устаревшее множество с
// нуля: seq scan по неиндексируемому rules_version (HOT-инвариант,
// класс T-104/T-175) осознанно полон — часовой тик ходит время-формой
// reanalyzeTickSQL (T-193). Имя — label || '.' || зона (стиль T-027)
// для Result.Name и логов (ТЗ 4.2).
const reanalyzeSelectSQL = "SELECT r.zone_id, r.domain_id, r.subdomain_id, r.variant, d.label || '.' || z.name " +
	"FROM subdomain_results AS r " +
	"JOIN domains AS d ON d.zone_id = r.zone_id AND d.id = r.domain_id " +
	"JOIN zones AS z ON z.id = r.zone_id " +
	"WHERE r.rules_version IS DISTINCT FROM $1::int4 " +
	"AND d.status <> $2::int2 " +
	"AND ($3::int2 IS NULL OR r.zone_id = $3) " +
	"AND (r.zone_id, r.domain_id, r.subdomain_id, r.variant) > ($4::int2, $5::int8, $6::int2, $7::int2) " +
	"ORDER BY r.zone_id, r.domain_id, r.subdomain_id, r.variant " +
	"LIMIT $8"

// reanalyzeTickSQL — пачка устаревших строк ВРЕМЯ-ФОРМЫ — ЧАСОВОЙ ТИК
// цикла (ReanalyzeTick, T-193 §4.1): тот же отбор, что
// reanalyzeSelectSQL (версия, охрана §13.1, фильтр зоны — паритет форм;
// тик ходит по всем зонам, параметр NULL), но курсор row-value > и
// ORDER BY — по композитному ключу (checked_at, PK): нижнюю границу
// checked_at планировщик PG16 выводит из row-value сам и обслуживает
// индексом subdomain_results_checked_idx (T-171) — тик читает только
// свежий срез вперёд курсора (окно ~ часа), а не heap всей таблицы
// (диагноз записи №12 журнала медленных 25.10: нулевой PK-курсор —
// Parallel Seq Scan 4,8 млн строк, 19,9 с, ради 56 строк). PK в хвосте
// ключа детерминирует пачки внутри одинаковых отметок (now() стабильна
// в транзакции записи — пачки строк делят отметку, T-058) — корректная
// пагинация без пропусков; checked_at в списке колонок — курсор
// следующей пачки (стиль retentionSelectSQL T-185; тайпкаст параметра
// времени — как там же).
const reanalyzeTickSQL = "SELECT r.zone_id, r.domain_id, r.subdomain_id, r.variant, d.label || '.' || z.name, r.checked_at " +
	"FROM subdomain_results AS r " +
	"JOIN domains AS d ON d.zone_id = r.zone_id AND d.id = r.domain_id " +
	"JOIN zones AS z ON z.id = r.zone_id " +
	"WHERE r.rules_version IS DISTINCT FROM $1::int4 " +
	"AND d.status <> $2::int2 " +
	"AND ($3::int2 IS NULL OR r.zone_id = $3) " +
	"AND (r.checked_at, r.zone_id, r.domain_id, r.subdomain_id, r.variant) > ($4::timestamptz, $5::int2, $6::int8, $7::int2, $8::int2) " +
	"ORDER BY r.checked_at, r.zone_id, r.domain_id, r.subdomain_id, r.variant " +
	"LIMIT $9"

// zoneIDByNameSQL — идентификатор зоны по имени для фильтра команды
// crawlerctl reanalyze --zone: имя хранится в punycode (EnsureZone,
// T-014), UNIQUE — не больше одной строки.
const zoneIDByNameSQL = "SELECT id FROM zones WHERE name = $1"

// RulesSource — источник набора правил для фонового цикла; удо­влетворяется
// *rules.Reloader (T-068). Узкий интерфейс — цикл не знает о SIGHUP и
// файлах, юнит-тесты подменяют фейком.
type RulesSource interface {
	// Set — текущий неизменяемый снапшот (nil не бывает у Reloader;
	// фейк без набора — проход пропускается с Warn).
	Set() *rules.Set
	// Subscribe — уведомления о смене набора: внеочередной проход.
	Subscribe() (<-chan *rules.Set, func())
}

// ReanalyzeOptions — параметры переанализа; нулевые значения заменяются
// дефолтами (withDefaults, стиль RecheckOptions T-030).
type ReanalyzeOptions struct {
	// Interval — период цикла ReanalyzeLoop (DefaultReanalyzeInterval);
	// на разовые проходы не влияет.
	Interval time.Duration
	// BatchSize — максимум строк в одной пачке (DefaultReanalyzeBatch).
	BatchSize int
	// Log — логгер цикла; nil — записи подавлены (slog.DiscardHandler).
	Log *slog.Logger
}

func (o ReanalyzeOptions) withDefaults() ReanalyzeOptions {
	if o.Interval <= 0 {
		o.Interval = DefaultReanalyzeInterval
	}
	if o.BatchSize <= 0 {
		o.BatchSize = DefaultReanalyzeBatch
	}
	return o
}

// ReanalyzeResult — итог прохода: строки выбраны/опубликованы. Разница
// Published < Selected — ошибка публикации посреди пачки (проход
// остановлен).
type ReanalyzeResult struct {
	Selected  int
	Published int
}

// resultKey — ключ строки subdomain_results (PK, ТЗ 6.1) и курсор
// пагинации дренажа.
type resultKey struct {
	zone    int16
	domain  int64
	sub     int16
	variant model.Variant
}

// ReanalyzeCursor — курсор тикового прохода по времени (T-193): последняя
// посещённая строка (checked_at, PK), переживающая проходы цикла —
// образец retentionCursor (T-185). Время ведёт (свежие эпизоды впереди:
// T-058 пишет now() сервера БД), PK детерминирует порядок внутри
// одинаковых отметок. Нулевое значение — «с якоря»: проход стартует с
// now() − DefaultReanalyzeLookback (§4.2: цикл заякоривает курсор при
// старте, ReanalyzeTick страхует нулевой; потеря курсора — рестарт —
// дёшева, одно окно). Поля не экспортированы: курсор двигает только
// ReanalyzeTick, владелец — цикл ReanalyzeLoop (одна горутина,
// блокировки не нужны — стиль retentionState T-185); внешние вызовы
// держат значение целиком (нулевая структура-литерал доступна и без
// полей).
type ReanalyzeCursor struct {
	checkedAt time.Time
	key       resultKey
}

// reanalyzeRow — строка пачки: ключ, имя домена для сообщения и отметка
// checked_at — курсор следующей пачки время-формы (время-форма читает
// её из строки; PK-форма дренажа колонку не выбирает — нулевая).
type reanalyzeRow struct {
	key       resultKey
	name      string
	checkedAt time.Time
}

// ReanalyzeOnce — один проход ПЕРВОЙ ПАЧКИ PK-формы (с нулевого
// ключа): выборка до opts.BatchSize устаревших строк и публикация
// Result в q:analyze по каждой (ТЗ 5.8 «Переанализ»). Повторное
// скачивание не выполняется. Проход идемпотентен на уровне «выборка+
// публикация»: после фиксации воркером T-071 новой rules_version строки
// перестают отбираться. Полный дренаж — ReanalyzeDrain, тиковый проход
// свежего среза — ReanalyzeTick (T-193).
func ReanalyzeOnce(ctx context.Context, pool db.DB, q queue.Queue, version int32, zone *int16, opts ReanalyzeOptions) (ReanalyzeResult, error) {
	if pool == nil {
		return ReanalyzeResult{}, errors.New("analyzer: переанализ: пул не задан")
	}
	if q == nil {
		return ReanalyzeResult{}, errors.New("analyzer: переанализ: очередь не задана")
	}
	rows, err := reanalyzeSelect(ctx, pool, version, zone, resultKey{}, opts.withDefaults().BatchSize)
	if err != nil {
		return ReanalyzeResult{}, err
	}
	out := ReanalyzeResult{Selected: len(rows)}
	if err := reanalyzePublish(ctx, q, rows, &out); err != nil {
		return out, err
	}
	return out, nil
}

// ReanalyzeDrain — ПОЛНЫЙ дренаж устаревшего множества пачками
// (DefaultReanalyzeBatch, PK-форма с нулевого курсора): выборка →
// публикация → курсор за последнюю строку пачки, до первой неполной
// пачки. Так смена правил (Subscribe цикла, T-068) и команда
// crawlerctl reanalyze проходят всё множество без повторов внутри
// прохода; «дорасти» до следующего прохода могут только строки, ставшие
// устаревшими после прохождения курсора.
func ReanalyzeDrain(ctx context.Context, pool db.DB, q queue.Queue, version int32, zone *int16, opts ReanalyzeOptions) (ReanalyzeResult, error) {
	if pool == nil {
		return ReanalyzeResult{}, errors.New("analyzer: переанализ: пул не задан")
	}
	if q == nil {
		return ReanalyzeResult{}, errors.New("analyzer: переанализ: очередь не задана")
	}
	o := opts.withDefaults()
	out := ReanalyzeResult{}
	after := resultKey{} // нулевой ключ меньше любого реального (zone_id ≥ 1)
	for {
		if err := ctx.Err(); err != nil {
			return out, fmt.Errorf("analyzer: переанализ: %w", err)
		}
		rows, err := reanalyzeSelect(ctx, pool, version, zone, after, o.BatchSize)
		if err != nil {
			return out, err
		}
		out.Selected += len(rows)
		if err := reanalyzePublish(ctx, q, rows, &out); err != nil {
			return out, err
		}
		if len(rows) < o.BatchSize {
			return out, nil // множество исчерпано
		}
		after = rows[len(rows)-1].key
	}
}

// ReanalyzeTick — тиковый проход цикла: дренаж СВЕЖЕГО СРЕЗА пачками
// (время-форма, T-193 §4.1) от курсора cursor до исчерпания окна —
// короткая или пустая пачка значит, что устаревшего вперёд курсора нет;
// пустая пачка окно исчерпала, курсор ждёт следующего тика. Курсор —
// состояние цикла, переживающее проходы (образец retentionCursor
// T-185): продвигается за последнюю строку каждой ОПУБЛИКОВАННОЙ пачки
// и только вперёд по (checked_at, PK) — стоимость прохода ∝ притоку с
// момента прошлого тика, а не размеру таблицы. Нулевой курсор — якорь
// now() − DefaultReanalyzeLookback (§4.2 — восстановление после потери
// курсора; цикл заякоривает курсор сам при старте, здесь — страховка
// разовых вызовов); nil — якорь без сохранения (разовый вызов без
// состояния). Ошибка публикации останавливает проход, не двигая курсор
// за сбойную пачку: повторный тик опубликует её строки снова
// (at-least-once, §3.4; дубликат ограничен пачкой, гасится
// идемпотентностью воркера T-071 по ключу). Отмена ctx — ошибка
// контекста с итогом опубликованного (стиль ReanalyzeDrain).
func ReanalyzeTick(ctx context.Context, pool db.DB, q queue.Queue, version int32, cursor *ReanalyzeCursor, opts ReanalyzeOptions) (ReanalyzeResult, error) {
	if pool == nil {
		return ReanalyzeResult{}, errors.New("analyzer: переанализ: пул не задан")
	}
	if q == nil {
		return ReanalyzeResult{}, errors.New("analyzer: переанализ: очередь не задана")
	}
	if cursor == nil {
		cursor = &ReanalyzeCursor{} // разовый вызов: якорь без сохранения
	}
	if cursor.checkedAt.IsZero() {
		// Якорь старта/восстановления (§4.2): now() − lookback, не эпоха —
		// страховка тика обслуживает свежие потери, старее — территория
		// полного дренажа (DefaultReanalyzeLookback, обоснование).
		cursor.checkedAt = time.Now().Add(-DefaultReanalyzeLookback)
	}
	o := opts.withDefaults()
	out := ReanalyzeResult{}
	for {
		if err := ctx.Err(); err != nil {
			return out, fmt.Errorf("analyzer: переанализ: %w", err)
		}
		rows, err := reanalyzeTickSelect(ctx, pool, version, *cursor, o.BatchSize)
		if err != nil {
			return out, err
		}
		if len(rows) == 0 {
			return out, nil // окно исчерпано — курсор ждёт следующего тика
		}
		out.Selected += len(rows)
		if err := reanalyzePublish(ctx, q, rows, &out); err != nil {
			return out, err
		}
		// Курсор — за последнюю строку опубликованной пачки.
		cursor.checkedAt, cursor.key = rows[len(rows)-1].checkedAt, rows[len(rows)-1].key
		if len(rows) < o.BatchSize {
			return out, nil // короткая пачка — устаревшее вперёд курсора кончилось
		}
	}
}

// reanalyzeSelect — пачка устаревших строк после курсора after
// (PK-форма, reanalyzeSelectSQL).
func reanalyzeSelect(ctx context.Context, pool db.DB, version int32, zone *int16, after resultKey, limit int) ([]reanalyzeRow, error) {
	var zoneArg any // nil → NULL параметр → фильтр зоны выключен
	if zone != nil {
		zoneArg = *zone
	}
	rows, err := pool.Query(ctx, reanalyzeSelectSQL,
		version,
		int16(model.StatusDeleted),
		zoneArg,
		int16(after.zone), int64(after.domain), int16(after.sub), int16(after.variant),
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("analyzer: переанализ: выборка: %w", err)
	}
	defer rows.Close()

	var out []reanalyzeRow
	for rows.Next() {
		var r reanalyzeRow
		var variant int16
		if err := rows.Scan(&r.key.zone, &r.key.domain, &r.key.sub, &variant, &r.name); err != nil {
			return nil, fmt.Errorf("analyzer: переанализ: выборка: чтение: %w", err)
		}
		r.key.variant = model.Variant(variant)
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("analyzer: переанализ: выборка: %w", err)
	}
	return out, nil
}

// reanalyzeTickSelect — пачка устаревших строк после курсора after
// (время-форма, reanalyzeTickSQL). Тик ходит по всем зонам — параметр
// зоны SQL всегда NULL (фильтр в форме сохранён для паритета с
// PK-формой, T-193 §4.1).
func reanalyzeTickSelect(ctx context.Context, pool db.DB, version int32, after ReanalyzeCursor, limit int) ([]reanalyzeRow, error) {
	rows, err := pool.Query(ctx, reanalyzeTickSQL,
		version,
		int16(model.StatusDeleted),
		nil, // зона не фильтруется — все
		after.checkedAt,
		int16(after.key.zone), int64(after.key.domain), int16(after.key.sub), int16(after.key.variant),
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("analyzer: переанализ: выборка тика: %w", err)
	}
	defer rows.Close()

	var out []reanalyzeRow
	for rows.Next() {
		var r reanalyzeRow
		var variant int16
		if err := rows.Scan(&r.key.zone, &r.key.domain, &r.key.sub, &variant, &r.name, &r.checkedAt); err != nil {
			return nil, fmt.Errorf("analyzer: переанализ: выборка тика: чтение: %w", err)
		}
		r.key.variant = model.Variant(variant)
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("analyzer: переанализ: выборка тика: %w", err)
	}
	return out, nil
}

// reanalyzePublish публикует Result по каждой строке пачки (q:analyze,
// формат T-024): только ключ и имя — контент воркер возьмёт из хранилища.
func reanalyzePublish(ctx context.Context, q queue.Queue, rows []reanalyzeRow, out *ReanalyzeResult) error {
	for i := range rows {
		msg := queue.Result{
			ZoneID:      rows[i].key.zone,
			DomainID:    rows[i].key.domain,
			SubdomainID: rows[i].key.sub,
			Variant:     rows[i].key.variant,
			Name:        rows[i].name,
		}
		if _, err := q.Publish(ctx, queue.StreamAnalyze, msg); err != nil {
			return fmt.Errorf("analyzer: переанализ: публикация q:analyze (домен %s, запись %d/%d): %w",
				rows[i].name, i+1, len(rows), err)
		}
		out.Published++
	}
	return nil
}

// ReanalyzeLoop — фоновый цикл переанализа (ТЗ 5.8 «Фоновая задача»):
// стартовый ПОЛНЫЙ дренаж пачками (подбирает устаревшее за простоем —
// стиль T-020/T-030/T-066; в том числе сменившиеся за простоем правила —
// Subscribe уведомляет только работающий процесс, T-068), далее тикер
// Interval — тиковые проходы время-формой: курсор заякоривается при
// старте цикла на now() − DefaultReanalyzeLookback (T-193 §4.2 — не с
// эпохи: первый тик не должен вычитывать всю историю) и переживает
// проходы, каждый тик продолжает с места остановки, а не сканирует
// таблицу с нуля; смена набора правил (Reloader.Subscribe, T-068:
// SIGHUP без рестарта) — внеочередной ПОЛНЫЙ дренаж, PK-курсор с нуля.
// Каждый проход берёт актуальную версию из src.Set(). Тик и дренаж
// идут в одной горутине select — пересечение множеств (смена правил во
// время тика) безопасно: оба идемпотентны на уровне «выборка+
// публикация» (T-071). Ошибка прохода — Warn, цикл живёт (контракт
// OnRun, T-012): опубликованное не теряется; неопубликованное дренажом
// вернёт следующая смена правил, тиком — повторный проход курсора
// (at-least-once, §3.4). Отмена ctx — чистый nil (graceful shutdown,
// §3.4). Курсор трогает только цикл (одна горутина) — блокировки не
// нужны (стиль retentionState T-185).
func ReanalyzeLoop(ctx context.Context, pool db.DB, q queue.Queue, src RulesSource, opts ReanalyzeOptions) error {
	if pool == nil {
		return errors.New("analyzer: переанализ: пул не задан")
	}
	if q == nil {
		return errors.New("analyzer: переанализ: очередь не задана")
	}
	if src == nil {
		return errors.New("analyzer: переанализ: источник набора правил не задан (rules.Reloader)")
	}
	o := opts.withDefaults()
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	sets, cancelSub := src.Subscribe()
	defer cancelSub()
	log.Info("цикл переанализа запущен",
		"interval", o.Interval.String(), "batch", o.BatchSize,
		"lookback", DefaultReanalyzeLookback.String())

	// Курсор тика — состояние цикла, переживающее проходы (T-193): якорь
	// ставится здесь, при старте цикла (§4.2 — «старт цикла — курсор
	// now() − lookback», не с эпохи: первый тик не вычитывает всю
	// историю); потеря курсора (рестарт) — тот же якорь в новом цикле,
	// ReanalyzeTick дополнительно якорит нулевой курсор сам.
	var cursor ReanalyzeCursor
	cursor.checkedAt = time.Now().Add(-DefaultReanalyzeLookback)

	// tick — тиковый проход: свежий срез от живого курсора (время-форма),
	// версия — актуальная на момент прохода.
	tick := func() {
		set := src.Set()
		if set == nil {
			log.Warn("проход переанализа: набор правил не загружен — пропуск")
			return
		}
		res, err := ReanalyzeTick(ctx, pool, q, set.Version, &cursor, o)
		if err != nil {
			// Отмена во время прохода — часть остановки, не тревога.
			if ctx.Err() == nil {
				log.Warn("проход переанализа", "err", err,
					"selected", res.Selected, "published", res.Published)
			}
			return
		}
		if res.Published > 0 {
			log.Info("проход переанализа",
				"rules_version", set.Version,
				"selected", res.Selected, "published", res.Published)
		}
	}

	// drain — полный дренаж: всё устаревшее множество пачками с нулевого
	// PK-курсора (смена правил — редкий, осознанно полный проход).
	drain := func() {
		set := src.Set()
		if set == nil {
			log.Warn("проход переанализа: набор правил не загружен — пропуск")
			return
		}
		res, err := ReanalyzeDrain(ctx, pool, q, set.Version, nil, o)
		if err != nil {
			// Отмена во время прохода — часть остановки, не тревога.
			if ctx.Err() == nil {
				log.Warn("проход переанализа", "err", err,
					"selected", res.Selected, "published", res.Published)
			}
			return
		}
		if res.Published > 0 {
			log.Info("проход переанализа",
				"rules_version", set.Version,
				"selected", res.Selected, "published", res.Published)
		}
	}

	drain() // стартовый дренаж: устаревшее за простоем, включая сменившиеся
	// за простоем правила (осознанно полный проход — §4.3); первый
	// тикер-тик продолжит курсором от якоря, поставленного выше.
	ticker := time.NewTicker(o.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case set, ok := <-sets:
			if !ok {
				// Эквивалентный мутант (T-M10): вырезание sets = nil
				// оставляет в select закрытый канал — он по-прежнему
				// немедленно готов, ok=false и continue вращают цикл
				// вхолостую без drain и логов, состояние и дальнейшие
				// развилки те же (различие только в нагрузке CPU —
				// детерминированно не наблюдаемо). Вырезание ветки
				// целиком закрыто тестом TestReanalyzeLoopClosedSets
				// (поток drain/логов по закрытому каналу).
				// mutator-disable-next-line statement/remove
				sets = nil // закрытый канал не должен зациклить select
				// Эквивалентный мутант (T-M10): continue → break лишь
				// выходит из select — цикл for продолжает следующей
				// итерацией, а тело ветки и так исчерпано: неразличимо.
				// mutator-disable-next-line loop/break
				continue
			}
			log.Info("смена набора правил — внеочередной переанализ",
				"rules_version", set.Version)
			drain()
		case <-ticker.C:
			tick()
		}
	}
}

// ZoneIDByName — идентификатор зоны по punycode-имени для фильтра
// команды crawlerctl reanalyze --zone. found = false — зоны в
// справочнике нет (ingest создаёт зоны при первом файле, T-014).
func ZoneIDByName(ctx context.Context, pool db.DB, name string) (id int16, found bool, err error) {
	if pool == nil {
		return 0, false, errors.New("analyzer: зона по имени: пул не задан")
	}
	rows, err := pool.Query(ctx, zoneIDByNameSQL, name)
	if err != nil {
		return 0, false, fmt.Errorf("analyzer: зона по имени: %w", err)
	}
	defer rows.Close()
	if rows.Next() {
		if err := rows.Scan(&id); err != nil {
			return 0, false, fmt.Errorf("analyzer: зона по имени: чтение: %w", err)
		}
		return id, true, nil
	}
	if err := rows.Err(); err != nil {
		return 0, false, fmt.Errorf("analyzer: зона по имени: %w", err)
	}
	return 0, false, nil
}
